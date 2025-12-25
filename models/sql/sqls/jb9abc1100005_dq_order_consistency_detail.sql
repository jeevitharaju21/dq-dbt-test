with header_ref as (
    select dq_header_sk, etl_nr
    from USAA_CSA.DBT_POC.dq_order_consistency_header
    where etl_nr = 0
),

mismatched_records as (
    select
        f.order_id,
        f.product_code,
        f.line_item_amount,
        f.tax_amount,
        f.gross_amount,
        h.dq_header_sk
    from USAA_CSA.DBT_POC.fct_order_analytics f
    join header_ref h on h.etl_nr = 0
    -- Logic: Detect where the sum of components does not equal the stored gross
    where ROUND(f.line_item_amount + f.tax_amount, 2) <> f.gross_amount
)

select
    HASH(md5(cast(coalesce(cast(order_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(product_code as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(0 as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))) as dq_detail_sk,
    dq_header_sk,
    current_date as controlm_o_dt,
    order_id::string || '-' || product_code as record_id,
    0 as etl_nr,
    current_timestamp as etl_recorded_gmts,
    gross_amount::string as src_failed_value_txt,
    'GROSS_AMOUNT' as src_failed_cde_txt,
    'ORDER_ID=' || order_id::string || '|PROD=' || product_code as key_cde_txt,
    (line_item_amount + tax_amount)::string as tgt_failed_value_txt,
    'EXPECTED_GROSS_AMOUNT' as tgt_failed_cde_txt
from mismatched_records


where etl_nr not in (select etl_nr from USAA_CSA.DBT_POC.dq_order_consistency_detail)
