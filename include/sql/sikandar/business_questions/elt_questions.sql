-- Business questions answered from the ELT mart layer (mart_*). One query per question.

-- Q1. Daily net revenue by zone and by store (DELIVERED only, after discount)
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id;

-- Q2. Revenue and gross margin by product category
SELECT * FROM mart_category_margin ORDER BY net_revenue DESC;

-- Q3. Average delivery time per store and % of delivered orders within 15 minutes
SELECT * FROM mart_delivery_sla ORDER BY store_id;

-- Q4a. Revenue by payment mode
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM mart_orders_clean WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC;

-- Q4b. Amount that does not reconcile with the payment gateway
SELECT coalesce(mismatch_reason, 'TOTAL') AS mismatch_reason, count(*) AS mismatches,
       round(sum(abs(difference)), 2) AS amount
FROM mart_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY grouping(mismatch_reason), 1;

-- Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)
SELECT * FROM mart_top_customers ORDER BY rank;

-- Q6. Rows rejected, by file and by reason (current staging state)
SELECT source_file, reason, rows_rejected FROM mart_dq_rejections ORDER BY source_file, rows_rejected DESC;
