{{ 
  config(
    materialized = 'incremental',
    unique_key   = 'dq_header_sk'
  ) 
}}

with failures as (
    select
        s.order_id
    from {{ ref('raw_order_item_src') }} s
    join {{ ref('tgt_sales') }} t
      on s.order_id = t.order_id
    group by s.order_id
    having sum(s.amount) <> max(t.total_amount)
),

final_summary as (
    select
        -- FIX: Removed quotes around var() to prevent SQL compilation error
       hash({{ dbt_utils.generate_surrogate_key([var('etl_nr', 0), "'TGT_SALES'"]) }}) as dq_header_sk,
        'TGT_SALES' as target_table_nm,
        'SOURCE_TARGET_SUM_CHECK' as dq_type_dc,
        current_date as controlm_o_dt,
        case 
            when count(*) = 0 then 'P' 
            else 'F' 
        end as status_cd,
        count(*) as failed_cnt,
        (select count(*) from {{ ref('tgt_sales') }}) as total_cnt,
        {{ var('etl_nr', 0) }} as etl_nr,
        current_timestamp as etl_recorded_gmts,
        'STG_ORDER_ITEM' as source_table_nm
    from failures
)

select * from final_summary

{% if is_incremental() %}
-- Safeguard to prevent duplicate header rows for the same batch
where etl_nr not in (
    select etl_nr from {{ this }}
)
{% endif %}