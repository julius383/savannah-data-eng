/* DuckDB SQL $input_file is a named query parameter passed
   by the Python code that executes this file */
select
  id as product_id,
  title as name,
  category,
  brand,
  price
from read_json($input_file)
where price > 50
