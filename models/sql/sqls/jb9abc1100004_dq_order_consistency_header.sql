with failures as (
    -- Validating that Gross Amount in Fact = (Line Amount + Tax)
    -- This catches calculation errors in the transformation layer
    select
        order_id,
        product_code
    from USAA_CSA.DBT_POC.fct_order_analytics
    where ROUND(line_item_amount + tax_amount, 2) <> gross_amount
),

final_summary as (
    select
        HASH(md5(cast(coalesce(cast(0 as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast('FCT_ORDER_ANALYTICS' as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))) as dq_header_sk,
        'FCT_ORDER_ANALYTICS' as target_table_nm,
        'GROSS_AMOUNT_INTEGRITY_CHECK' as dq_type_dc,
        current_date as controlm_o_dt,
        case 
            when count(*) = 0 then 'P' 
            else 'F' 
        end as status_cd,
        count(*) as failed_cnt,
        (select count(*) from USAA_CSA.DBT_POC.fct_order_analytics) as total_cnt,
        0 as etl_nr,
        current_timestamp as etl_recorded_gmts,
        'FCT_ORDER_ANALYTICS' as source_table_nm
    from failures
)

select * from final_summary


where etl_nr not in (select etl_nr from USAA_CSA.DBT_POC.dq_order_consistency_header)
