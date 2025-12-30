{{ config(materialized='incremental', unique_key='payment_id') }}

select
payment_id,
order_id,
payment_mode,
payment_status,
payment_amount,
payment_ts,
updated_ts
from DBT_POC.RAW_PAYMENT_SRC