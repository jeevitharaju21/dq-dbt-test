{{ config(materialized='incremental') }}

select * from {{ ref('int_sales_order') }}