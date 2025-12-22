{{ 
  config(
    materialized = 'incremental',     -- Append new failure rows per run
    unique_key   = 'dq_detail_sk'     -- Ensure uniqueness per failed record
  ) 
}}

-- ==========================================================
-- Step 1: Calculate SOURCE aggregate values
-- ==========================================================
-- s.src_total_amount = SUM(amount) from STG_ORDER_ITEM

-- ==========================================================
-- Step 2: Get TARGET values
-- ==========================================================
-- t.tgt_total_amount = TOTAL_AMOUNT from TGT_SALES

-- ==========================================================
-- Step 3: Compare source vs target
-- Insert ONLY mismatched records into DQ_DETAIL
-- ==========================================================
select
    -- Unique surrogate key for each failed record
    {{ dbt_utils.generate_surrogate_key(['s.order_id','current_timestamp']) }} as dq_detail_sk,

    -- Foreign key reference to DQ_HEADER
    h.dq_header_sk,

    -- Control-M / batch date
    current_date as controlm_o_dt,

    -- Business key identifying the failed record
    s.order_id::string as record_id,

    -- ETL run identifier
    {{ var('etl_nr') }} as etl_nr,

    -- Timestamp of DQ logging
    current_timestamp as etl_recorded_gmts,

    -- Source value that caused the failure
    s.src_total_amount::string as src_failed_value_txt,

    -- Column name that failed in source
    'TOTAL_AMOUNT' as src_failed_cde_txt,

    -- Key information for traceability
    'ORDER_ID=' || s.order_id as key_cde_txt,

    -- Target value that does not match source
    t.tgt_total_amount::string as tgt_failed_value_txt,

    -- Column name that failed in target
    'TOTAL_AMOUNT' as tgt_failed_cde_txt

from (

    -- ======================================================
    -- Source aggregation (expected values)
    -- ======================================================
    select
        order_id,
        sum(amount) as src_total_amount
    from {{ ref('stg_order_item') }}
    group by order_id

) s

join (

    -- ======================================================
    -- Target values (actual loaded data)
    -- ======================================================
    select
        order_id,
        total_amount as tgt_total_amount
    from {{ ref('tgt_sales') }}

) t
  on s.order_id = t.order_id

-- ==========================================================
-- Join to DQ_HEADER to link detail records to summary
-- ==========================================================
join {{ ref('dq_sales_consistency_header') }} h
  on h.etl_nr = {{ var('etl_nr') }}

-- ==========================================================
-- Only log rows where source and target values mismatch
-- ==========================================================
where s.src_total_amount <> t.tgt_total_amount

-- ==========================================================
-- Incremental safeguard
-- Prevent duplicate detail records for same ETL run
-- ==========================================================
{% if is_incremental() %}
and {{ var('etl_nr') }} not in (
    select etl_nr from {{ this }}
)
{% endif %}
