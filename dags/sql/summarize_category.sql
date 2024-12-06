/* BigQuery SQL expects to be run in the appropriate
   context i.e within a defined project and dataset otherwise
   all table names need to be fully qualified in line with
   the env variables GCP_PROJECT and BQ_DATASET so products_table
   below becomes GCP_PROJECT.BQ_DATASET.products_table.
*/
with all_categories as (-- find all product categories
    select distinct category
    from products_table
),

-- aggregate number of items sold and total value of sales
category_aggregates as (
    select
        category,
        sum(quantity) as items_sold,
        sum(quantity * price) as total_sales
    from
        (
            select
                c.product_id,
                c.price,
                c.quantity,
                p.category
            from carts_table c
            inner join products_table p on c.product_id = p.product_id
            order by 1
        )
    group by category
)


select
    ac.category,
    -- set default values for categories without any sales
    coalesce(items_sold, 0) as items_sold,
    coalesce(total_sales, 0) as total_sales
from all_categories ac
left join category_aggregates ca on ac.category = ca.category
order by
    2,
    3
