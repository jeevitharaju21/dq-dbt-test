select
o.order_id,
o.customer_id,
c.customer_name,
c.country_code,
sum(i.quantity) as total_qty,
sum(i.amount) as total_amount,
p.payment_status
from {{ ref('stg_order') }} o
join {{ ref('stg_customer') }} c on o.customer_id = c.customer_id
join {{ ref('stg_order_item') }} i on o.order_id = i.order_id
join {{ ref('stg_payment') }} p on o.order_id = p.order_id
group by
o.order_id, o.customer_id, c.customer_name, c.country_code, p.payment_status

