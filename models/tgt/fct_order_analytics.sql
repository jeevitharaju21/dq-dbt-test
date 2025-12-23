{{ config(materialized='incremental') }}

select * from {{ ref('int_fct_order') }}