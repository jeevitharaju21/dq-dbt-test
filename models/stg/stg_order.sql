{{ config(materialized='incremental', unique_key='order_id') }}


select
order_id,
customer_id,
order_date,
order_status,
channel,
created_ts,
updated_ts
from {{ source('raw','order') }}


{% if is_incremental() %}
where updated_ts > (select max(updated_ts) from {{ this }})
{% endif %}