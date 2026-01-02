{{ config(materialized='table') }}

SELECT 
    o.ORDER_ID,
    c.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    c.COUNTRY_CODE,
    o.ORDER_DATE,
    o.CREATED_TS AS ORDER_CREATED_TS,
    p.PAYMENT_TS,
    DATEDIFF('minute', o.CREATED_TS, p.PAYMENT_TS) AS PAY_LATENCY_MINUTES,
    DATEDIFF('day', o.CREATED_TS, o.UPDATED_TS) AS PROCESSING_DAYS,
    oi.PRODUCT_CODE,
    oi.QUANTITY,
    oi.UNIT_PRICE,
    oi.AMOUNT AS LINE_ITEM_AMOUNT,
    ROUND(oi.AMOUNT * 0.10, 2) AS TAX_AMOUNT,
    ---failed scenario check-----------------
    CASE 
        WHEN c.COUNTRY_CODE = 'US' THEN ROUND(oi.AMOUNT * 1.15, 2) -- WRONG 
        ELSE ROUND(oi.AMOUNT * 1.10, 2)                           -- CORRECT
    END AS GROSS_AMOUNT,
  --  ROUND(oi.AMOUNT * 1.12, 2) AS GROSS_AMOUNT,
    o.ORDER_STATUS,
    o.CHANNEL,
    p.PAYMENT_MODE,
    p.PAYMENT_STATUS,
    CASE 
        WHEN oi.AMOUNT > 200 THEN 'HIGH_VALUE'
        WHEN oi.AMOUNT BETWEEN 50 AND 200 THEN 'MID_VALUE'
        ELSE 'LOW_VALUE'
    END AS ORDER_VALUE_SEGMENT,
    
    CASE 
        WHEN o.ORDER_STATUS = 'CANCELLED' OR p.PAYMENT_STATUS = 'FAILED' THEN 'REVENUE_LOST'
        WHEN p.PAYMENT_STATUS = 'COMPLETED' THEN 'REVENUE_REALIZED'
        ELSE 'REVENUE_PENDING'
    END AS REVENUE_IMPACT_STATUS
, CONCAT(o.order_id, oi.product_code) as fct_order_sk
FROM {{ ref('raw_order_src') }} o
JOIN {{ ref('raw_customer_src') }} c ON o.CUSTOMER_ID = c.CUSTOMER_ID
JOIN {{ ref('raw_order_item_src') }} oi ON o.ORDER_ID = oi.ORDER_ID
LEFT JOIN {{ ref('raw_payment_src') }} p ON o.ORDER_ID = p.ORDER_ID