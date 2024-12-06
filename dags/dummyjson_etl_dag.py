import json
import logging
import os
import re
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Callable, TypeAlias

import duckdb
import requests
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery, storage
from toolz import flip


DATASET_DIR = Path("datasets")

Spec: TypeAlias = dict[str, Callable | "Spec"]

GCP_BUCKET = os.getenv("GCP_BUCKET")
GCP_PROJECT = os.getenv("GCP_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET")

logger = logging.getLogger(__name__)


def check_spec(spec: Spec, item: dict) -> bool:
    """Validate thet the keys and values of a dict match a particular specification.

    Args:
        spec (Spec): A dict with keys corresponding to those in item. The values for
            each key are either a simple boolean funtion or another Spec that can be
            used to validate nested dicts.
        item (dict): the item we want to validate.

    Returns:
        bool: True if spec matches and False otherwise.
    """
    valid = True
    for key, validator in spec.items():
        if not valid:  # exit early if any validators failed
            return False
        if key not in item:  # we assume every key part of spec must be in item
            return False
        if callable(
            validator
        ):  # check if function returns true and update validity
            if isinstance(item[key], list):
                # validator automatically run on all values of lists
                valid = valid and all([validator(i) for i in item[key]])
            else:
                valid = valid and validator(item[key])
        elif isinstance(validator, dict):  # validate nested dict
            if isinstance(item[key], list):
                valid = valid and all(
                    [check_spec(validator, i) for i in item[key]]
                )
            else:
                valid = valid and check_spec(validator, item[key])
        else:
            raise ValueError(
                f"Validator of type {isinstance(validator)} is not supported."
            )
    return valid


def write_data(name, items):
    if not DATASET_DIR.exists():
        DATASET_DIR.mkdir()
    ofile = DATASET_DIR / f"{name}.json"
    if ofile.exists():
        ofile.unlink()
    with open(ofile, "w") as fp:
        json.dump(items, fp)
    return str(ofile)


def read_data(path):
    with open(path, "r") as fp:
        data = json.load(fp)
    return data


@dag(
    start_date=datetime(2024, 12, 2),
    schedule="@daily",
    catchup=False,
    default_args={
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "retries": 3,
    },
)
def dummyjson_etl_dag():
    @task
    def fetch(api_url, max_limit=0):
        """Fetch data from an API endpoint.

        Args:
            api_url (str): a URL where we can find a resource.
            max_limit (int): how many of a resource should we try to
                get. max_limit=0 means try and get all available

        Returns:
            list[dict]: values deserialized from JSON response of call to
                api_url.
        """
        try:
            resource_type = Path(api_url).name
            results = []
            with requests.Session() as s:
                skip = 0
                limit = 20
                items = None
                while True and (max_limit == 0 or len(results) < max_limit):
                    payload = {"limit": limit, "skip": skip}
                    resp = s.get(api_url, params=payload)
                    resp.raise_for_status()
                    json_out = resp.json()
                    if items := json_out[resource_type]:
                        skip += len(items)
                        results.extend(items)
                        continue
                    else:
                        break
        except requests.HTTPError as e:
            logger.exception(
                "Error at trying to fetch %s data", resource_type, exc_info=e
            )
            raise
        path = write_data(f"raw_{resource_type}", results)
        logger.info("Wrote results for fetching %s to %s", resource_type, path)
        return path

    @task
    def validate(spec: Spec, data_file, target):
        """Use spec to validate multiple items from data_file."""
        data = read_data(data_file)
        validated = []
        for item in data:
            if check_spec(spec, item):
                validated.append(item)
            else:
                logger.error("Validation for item %s failed", item)
        path = write_data(f"validated_{target}", validated)
        logger.info("Wrote validated results for %s to %s", target, path)
        return path

    @task
    def transform(data_file: str, target: str):
        """Run pre-defined SQL transformations on data in data_file."""
        con = duckdb.connect(":memory:conn1")
        transformation = Path(__file__).parent / f"sql/transform_{target}.sql"
        ofile = str(DATASET_DIR / f"cleaned_{target}.csv")
        with open(transformation, "r") as fp:
            sql = fp.read()
            rs = con.sql(sql, params={"input_file": data_file})
            rs.write_csv(ofile)
            logger.info("Wrote transformed results for %s to %s", target, ofile)
        return ofile

    @task
    def upload_to_gcs(source_filename: str, dest_filename: str):
        "Upload file to Google Cloud Storage."
        client = storage.Client(project=GCP_PROJECT)
        bucket = client.bucket(GCP_BUCKET)
        blob = bucket.blob(dest_filename)
        blob.upload_from_filename(source_filename)
        logger.info(
            "Uploaded %s to  %s",
            source_filename,
            f"gs://{GCP_BUCKET}/{dest_filename}",
        )
        return dest_filename

    @task
    def load_to_bigquery(schema, csv_filename, target):
        """Load data from file to bigquery table

        Args:
            schema (list[bigquery.SchemaField]): bigquery schema definition for
                table to create/overwrite.
            csv_filename (str): filename of csv on the default configured bucket.
            target (str): tag used to give created table meaningful name.
        """
        client = bigquery.Client(project=GCP_PROJECT)
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        )
        uri = f"gs://{GCP_BUCKET}/{csv_filename}"
        table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{target}_table"

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )
        load_job.result()
        logger.info("Loaded data from %s to BigQuery table %s", uri, table_id)
        return

    @task
    def generate_summary(sql_file, output_table):
        """Run pre-defined SQL code and save results to a bigquery table."""
        with open(Path(__file__).parent / sql_file) as fp:
            sql = fp.read()
            client = bigquery.Client(project=GCP_PROJECT)
            table_id = f"{GCP_PROJECT}.{BQ_DATASET}.{output_table}"
            job_config = bigquery.QueryJobConfig(
                destination=table_id,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                default_dataset=f"{GCP_PROJECT}.{BQ_DATASET}",
            )
            query_job = client.query(sql, job_config=job_config)
            query_job.result()
            logger.info(
                "Generated summary based on %s at %s", sql_file, table_id
            )
        return

    resource_config = {
        "users": {
            "api_url": "https://dummyjson.com/users",
            "target": "users",
            "cloud_filename": "users.csv",
            "validation_spec": {
                "id": flip(isinstance, int),
                "firstName": flip(isinstance, str),
                "lastName": flip(isinstance, str),
                "age": lambda x: isinstance(x, int) and x > 0,
                "gender": flip(isinstance, str),
                "address": {
                    "address": lambda x: isinstance(x, str)
                    and bool(partial(re.match, r"Street$")),
                    "city": str,
                    "postalCode": lambda x: isinstance(x, str)
                    and bool(partial(re.match, r"[0-9]{5}")),
                },
            },
            "bigquery_schema": [
                bigquery.SchemaField("user_id", "INTEGER"),
                bigquery.SchemaField("first_name", "STRING"),
                bigquery.SchemaField("last_name", "STRING"),
                bigquery.SchemaField("gender", "STRING"),
                bigquery.SchemaField("age", "INTEGER"),
                bigquery.SchemaField("street", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("postal_code", "STRING"),
            ],
        },
        "products": {
            "api_url": "https://dummyjson.com/products",
            "target": "products",
            "cloud_filename": "products.csv",
            "validation_spec": {
                "id": flip(isinstance, int),
                "title": flip(isinstance, str),
                "category": flip(isinstance, str),
                "brand": flip(isinstance, str),
                "price": lambda x: isinstance(x, float) and x > 0,
            },
            "bigquery_schema": [
                bigquery.SchemaField("product_id", "INTEGER"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("category", "STRING"),
                bigquery.SchemaField("brand", "STRING"),
                bigquery.SchemaField("price", "NUMERIC"),
            ],
        },
        "carts": {
            "api_url": "https://dummyjson.com/carts",
            "target": "carts",
            "cloud_filename": "carts.csv",
            "validation_spec": {
                "id": flip(isinstance, int),
                "products": {
                    "id": flip(isinstance, int),
                    "quantity": lambda x: isinstance(x, int) and x > 0,
                    "price": lambda x: isinstance(x, float) and x > 0,
                },
                "userId": flip(isinstance, int),
            },
            "bigquery_schema": [
                bigquery.SchemaField("cart_id", "INTEGER"),
                bigquery.SchemaField("user_id", "INTEGER"),
                bigquery.SchemaField("product_id", "INTEGER"),
                bigquery.SchemaField("quantity", "INTEGER"),
                bigquery.SchemaField("price", "NUMERIC"),
                bigquery.SchemaField("total_cart_value", "NUMERIC"),
            ],
        },
    }
    data_tasks = {}

    for resource, config in resource_config.items():
        # fmt: off
        api_url        = config["api_url"]
        spec           = config["validation_spec"]
        cloud_filename = config["cloud_filename"]
        schema         = config["bigquery_schema"]
        # fmt: on
        with TaskGroup(group_id=f"{resource}_etl") as tg:
            raw = fetch(api_url=api_url)
            validated = validate(spec=spec, data_file=raw, target=resource)
            transformed = transform(data_file=validated, target=resource)
            csv_filename = upload_to_gcs(
                source_filename=transformed,
                dest_filename=cloud_filename,
            )
            load_to_bigquery(
                schema=schema,
                csv_filename=csv_filename,
                target=resource,
            )
            # save TaskGroups for declaring dependencies in later tasks
            data_tasks[resource] = tg

    # fmt: off
    summary_config = {
        "user": {
            "summary_definition": "sql/summarize_user.sql",
            "bigquery_table"    : "user_summary_table",
            "deps"              : ["users", "carts"],
        },
        "category": {
            "summary_definition": "sql/summarize_category.sql",
            "bigquery_table"    : "category_summary_table",
            "deps"              : ["products", "carts"],
        },
    }
    # fmt: on

    for summary, config in summary_config.items():
        with TaskGroup(group_id=f"{summary}_summary") as tg:
            generate_summary(
                config["summary_definition"], config["bigquery_table"]
            )
            for dep in config["deps"]:
                data_tasks[dep] >> tg


dummyjson_etl_dag()
