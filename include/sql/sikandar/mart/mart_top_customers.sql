-- Q5: top 10 customers by spend (DELIVERED net revenue), loyalty tier, no raw email or phone.
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    row_number() OVER (ORDER BY sum(o.net_revenue) DESC, o.customer_id) AS rank,
    o.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
    count(*)                       AS delivered_orders,
    round(sum(o.net_revenue), 2)   AS total_spend
FROM mart_orders_clean o
JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status <> 'FAIL'
WHERE o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier
ORDER BY rank
LIMIT 10;
