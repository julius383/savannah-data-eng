/* DuckDB SQL $input_file is a named query parameter passed
   by the Python code that executes this file */
with cart_items as (
  select
    id as cart_id,
    userId as user_id,
    unnest(products).id as product_id,
    unnest(products).quantity as quantity,
    unnest(products).price as price
  from read_json($input_file)
), cart_totals as (
  select cart_id, round(sum(quantity * price), 2) as total_cart_value
  from cart_items
  where price > 50
  group by cart_id
)
select
  ci.cart_id,
  ci.user_id,
  ci.product_id,
  ci.quantity,
  ci.price,
  ct.total_cart_value
from cart_items ci
inner join cart_totals ct on ct.cart_id = ci.cart_id
