{{ config(materialized='incremental', unique_key='item_id') }}


select
order_id,
item_id,
product_code,
quantity,
unit_price,
amount,
updated_ts
from {{ source('raw','order_item') }}


{% if is_incremental() %}
where updated_ts > (select max(updated_ts) from {{ this }})
{% endif %}