# Savannah Informatics Data Engineering Task

This is an implementation of the [Savannah Informatics][10] Data Engineer Interview
task. Processes data from [DummyJSON][3] making use of:

- [Apache Airflow][4] - workflow orchestration.
- [Astro CLI][1] - project organization and setup.
- [Google Cloud Storage][5] and [Google BigQuery][6]  - data storage/analysis.
- [DuckDB][7] - SQL based data transformations.


## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Usage](#usage)

## Overview

```txt
├──  dags                              # Airflow DAGs directory
│   ├──  sql
│   │   ├──  summarize_category.sql    # aggregate sales by category
│   │   ├──  summarize_user.sql        # aggregate sale value and quantity by user
│   │   ├──  transform_carts.sql       # transform carts, products and users for into final forms
│   │   ├──  transform_products.sql
│   │   └──  transform_users.sql
│   └──  dummyjson_etl_dag.py          # Airflow DAG definition. Contains main application logic
├──  datasets                          # where tasks in dummyjson_etl_dag.py store intermediate files
├──  docs
│   ├──  design.md                     # detailed description of project's design and implementation
│   ├── 󱁉 dummyjson_dag_graph.dot       # graphviz graph of Airflow DAG
│   └──  viz.py                        # simple visualization of summaries
├──  include                           # put any files to include in airflow environment here
├──  plugins                           # for custom or community Airflow plugins
├──  tests
├──  docker-compose.override.yml       # necessary for to setup cloud authentication
├──  Dockerfile                        # Astro Runtime Docker Image
├──  packages.txt                      # put OS-level packages required by DAGs here
├──  README.md
└──  requirements.txt                  # Python packages required by DAGs
```

## Installation

Running the code in this project requires installing the [Astro CLI][1] which
depends on the presence of a container service such as Docker on your system.
Detailed install instructions for each operating system can
be found on the project's [website]. On Linux installation is as simple as
running:

```sh
$ curl -sSL install.astronomer.io | sudo bash -s
```

After the above command finishes you should have the Astro CLI installed
somewhere on your system.

```sh
$ which astro
/usr/local/bin/astro
```

## Usage

Before we can use `astro` to start our project we need to make some
configuration changes.

1. Rename `.env.example` to `.env` and change the following values, leaving
   `GOOGLE_APPLICATION_CREDENTIALS` intact.
    - `GCP_PROJECT` - Google Cloud Platform Project ID [^1].
    - `GCP_BUCKET`  - Google Cloud Storage bucket ID where cleaned data is saved [^2].
    - `BQ_DATASET`  - Google BigQuery Dataset where tables containing cleaned
      data and summary results are created [^3].
1. Replace `/home/kajm/.config/gcloud/application_default_credentials.json`
    in `docker-compose.override.yml` with the location of the [Application Default
    Credentials][8] for Google Cloud on your own system. The process of configuring
    the credentials is a straightforward process described in detail [here][9].
    See also Astro's page about [Cloud Authentication][11]

Finally we can launch our project using:

```sh
astro dev start
```

Once this command exits we can load up the Airflow Dashboard by navigating to
the URL that appears at the end of the output, usually `localhost:8080`. You
should see the `dummyjson_etl_dag` that can be run. Details of what exactly the
DAG can be found in [docs/design.md](docs/design.md)


[1]: https://www.astronomer.io/docs/astro/cli/overview
[2]: https://www.astronomer.io/docs/astro/cli/install-cli?tab=linux#install-the-astro-cli
[3]: https://dummyjson.com/
[4]: https://airflow.apache.org/
[5]: https://cloud.google.com/storage/?hl=en
[6]: https://cloud.google.com/bigquery/?hl=en
[7]: https://duckdb.org
[8]: https://cloud.google.com/docs/authentication/application-default-credentials
[9]: https://cloud.google.com/docs/authentication/provide-credentials-adc
[10]: https://www.savannahinformatics.com/
[11]: https://www.astronomer.io/docs/astro/cli/authenticate-to-clouds/?tab=gcp#optional-test-your-credentials-with-a-secrets-backend

[^1]: https://cloud.google.com/docs/overview
[^2]: https://cloud.google.com/storage/docs/introduction
[^3]: https://cloud.google.com/bigquery/docs
[^4]: https://www.astronomer.io/docs/astro/cli/authenticate-to-clouds/?tab=gcp#optional-test-your-credentials-with-a-secrets-backend
