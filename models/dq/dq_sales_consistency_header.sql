{{ 
  config(
    materialized = 'incremental',      -- Insert one row per dbt run
    unique_key   = 'dq_header_sk'       -- Prevent duplicate headers for same run
  ) 
}}

-- ==========================================================
-- Step 1: Identify FAILED business keys (ORDER_ID)
-- Logic:
--   Compare SUM(amount) from source (STG_ORDER_ITEM)
--   against TOTAL_AMOUNT in target (TGT_SALES)
--   Any mismatch = DQ failure
-- ==========================================================
with failures as (

    select
        s.order_id
    from {{ ref('stg_order_item') }} s      -- Source table
    join {{ ref('tgt_sales') }} t           -- Target table
      on s.order_id = t.order_id
    group by s.order_id
    having sum(s.amount) <> max(t.total_amount)

)

-- ==========================================================
-- Step 2: Insert ONE summary record into DQ_HEADER
-- This represents the overall DQ status for this run
-- ==========================================================
select
    -- Surrogate key for DQ_HEADER (unique per run)
    {{ dbt_utils.generate_surrogate_key(['current_timestamp']) }} as dq_header_sk,

    -- Target table being validated
    'TGT_SALES' as target_table_nm,

    -- Type of data quality check
    'SOURCE_TARGET_SUM_CHECK' as dq_type_dc,

    -- Control-M / batch processing date
    current_date as controlm_o_dt,

    -- Pass if no failures, Fail if at least one mismatch
    case 
        when count(*) = 0 then 'P' 
        else 'F' 
    end as status_cd,

    -- Number of failed business keys
    count(*) as failed_cnt,

    -- Total records in target table (for reporting)
    (select count(*) from {{ ref('tgt_sales') }}) as total_cnt,

    -- ETL run identifier passed from scheduler
    {{ var('etl_nr') }} as etl_nr,

    -- Timestamp when DQ was recorded
    current_timestamp as etl_recorded_gmts,

    -- Source table(s) involved in the check
    'STG_ORDER_ITEM' as source_table_nm

from failures

-- ==========================================================
-- Step 3: Incremental safeguard
-- Prevents inserting multiple header rows for same ETL run
-- ==========================================================
{% if is_incremental() %}
where {{ var('etl_nr') }} not in (
    select etl_nr from {{ this }}
)
{% endif %}
