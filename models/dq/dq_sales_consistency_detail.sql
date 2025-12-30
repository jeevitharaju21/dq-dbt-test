{{ 
  config(
    materialized = 'incremental',
    unique_key   = 'dq_detail_sk'
  )
}}

with source_aggr as (
    select
        order_id,
        sum(amount) as src_total_amount
    from {{ ref('raw_order_item_src') }}
    group by order_id
),

target_data as (
    select
        order_id,
        total_amount as tgt_total_amount
    from {{ ref('tgt_sales') }}
),

header_ref as (
    -- Reference the header we just created to get the FK
    select 
        dq_header_sk,
        etl_nr
    from {{ ref('dq_sales_consistency_header') }}
    where etl_nr = {{ var('etl_nr', 0) }}
),

mismatched_records as (
    select
        s.order_id,
        s.src_total_amount,
        t.tgt_total_amount,
        h.dq_header_sk
    from source_aggr s
    join target_data t on s.order_id = t.order_id
    join header_ref h on h.etl_nr = {{ var('etl_nr', 0) }}
    where s.src_total_amount <> t.tgt_total_amount
)

select
    hash({{ dbt_utils.generate_surrogate_key(['order_id', var('etl_nr', 0)]) }}) as dq_detail_sk,
    dq_header_sk,
    current_date as controlm_o_dt,
    order_id::string as record_id,
    {{ var('etl_nr', 0) }} as etl_nr,
    current_timestamp as etl_recorded_gmts,
    src_total_amount::string as src_failed_value_txt,
    'TOTAL_AMOUNT' as src_failed_cde_txt,
    'ORDER_ID=' || order_id::string as key_cde_txt,
    tgt_total_amount::string as tgt_failed_value_txt,
    'TOTAL_AMOUNT' as tgt_failed_cde_txt
from mismatched_records

{% if is_incremental() %}
where etl_nr not in (
    select etl_nr from {{ this }}
)
{% endif %}