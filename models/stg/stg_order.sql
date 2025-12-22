{{ config(materialized='incremental', unique_key='order_id') }}


select
order_id,
customer_id,
order_date,
order_status,
channel,
created_ts,
updated_ts
from DBT_POC.RAW_ORDER_SRC


{% if is_incremental() %}
where updated_ts > (select max(updated_ts) from {{ this }})
{% endif %}