/* DuckDB SQL $input_file is a named query parameter passed
   by the Python code that executes this file */
select
  id as user_id,
  firstName as first_name,
  lastName as last_name,
  gender,
  age,
  regexp_extract(address.address, '.+\s+([a-zA-Z]+\s+Street)$', 1) as street,
  address.city as city,
  address.postalCode as postal_code
from read_json($input_file)
