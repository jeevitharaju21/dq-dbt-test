{{ config(materialized='incremental', unique_key='customer_id') }}


select
customer_id,
customer_name,
email_id,
country_code,
customer_status,
created_ts,
updated_ts
from {{ source('raw','customer') }}


{% if is_incremental() %}
where updated_ts > (select max(updated_ts) from {{ this }})
{% endif %}