/* BigQuery SQL expects to be run in the appropriate
   context i.e within a defined project and dataset otherwise
   all table names need to be fully qualified in line with
   the env variables GCP_PROJECT and BQ_DATASET so users_table
   below becomes GCP_PROJECT.BQ_DATASET.users_table.
*/
with item_totals as ( -- how many items did each user buy
    select
        user_id,
        sum(quantity) as total_items
    from carts_table
    group by user_id
),

spent_totals as (   -- how much did each user spent
    select
        user_id,
        sum(total_cart_value) as total_spent
    from
        (select
            user_id,
            cart_id,
            total_cart_value,
            /* window funtion used to only find single product within each cart
               since they all have the correct total_cart_value
               but we only count cart once */
            row_number() over (partition by user_id, cart_id) as rn
        from carts_table
        qualify rn = 1
        order by 1)
    group by user_id
)

select
    u.user_id,
    u.first_name,
    st.total_spent,
    it.total_items,
    u.age,
    u.city
from users_table u
join spent_totals st on u.user_id = st.user_id
join item_totals it on u.user_id = it.user_id
order by 1
