select
o.order_id,
o.customer_id,
c.customer_name,
c.country_code,
sum(i.quantity) as total_qty,
sum(i.amount) as total_amount,
p.payment_status
from {{ ref('raw_order_src') }} o
join {{ ref('raw_customer_src') }} c on o.customer_id = c.customer_id
join {{ ref('raw_order_item_src') }} i on o.order_id = i.order_id
join {{ ref('raw_payment_src') }} p on o.order_id = p.order_id
group by
o.order_id, o.customer_id, c.customer_name, c.country_code, p.payment_status

