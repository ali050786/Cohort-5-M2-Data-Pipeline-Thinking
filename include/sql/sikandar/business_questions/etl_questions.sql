-- Business questions answered from the ETL star schema (etl_*). One query per question.

-- Q1. Daily net revenue by zone and by store (DELIVERED only, after discount)
SELECT f.order_date, s.zone, s.store_id, s.store_name, count(*) AS delivered_orders,
       round(sum(f.net_revenue), 2) AS net_revenue
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY f.order_date, s.zone, s.store_id;

-- Q2. Revenue and gross margin by product category
SELECT * FROM etl_category_margin ORDER BY net_revenue DESC;

-- Q3. Average delivery time per store and % of delivered orders within 15 minutes
SELECT s.store_id, s.store_name, s.zone, count(*) AS delivered_orders,
       round(avg(f.delivery_minutes), 1) AS avg_delivery_minutes,
       round(100.0 * avg(CAST(f.within_15_min AS INTEGER)), 1) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY s.store_id;

-- Q4a. Revenue by payment mode
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC;

-- Q4b. Amount that does not reconcile with the payment gateway
SELECT coalesce(mismatch_reason, 'TOTAL') AS mismatch_reason, count(*) AS mismatches,
       round(sum(abs(difference)), 2) AS amount
FROM etl_payment_reconciliation GROUP BY ROLLUP (mismatch_reason) ORDER BY grouping(mismatch_reason), 1;

-- Q5. Top 10 customers by spend, with loyalty tier (masked contact details only)
SELECT c.customer_id, c.customer_name_masked, c.email_masked, c.phone_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED'
GROUP BY ALL ORDER BY total_spend DESC LIMIT 10;

-- Q6. Rows rejected in today's run, by file and by reason
SELECT source_file, reason, count(*) AS rows_rejected
FROM (SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason
      FROM etl_rejections
      WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_sikandar'
                      ORDER BY end_time DESC LIMIT 1))
GROUP BY ALL ORDER BY source_file, rows_rejected DESC;
