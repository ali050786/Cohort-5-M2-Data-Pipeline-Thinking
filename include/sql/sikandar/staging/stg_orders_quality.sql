-- Recompute the quality flags on every stg_orders row (rules may reference refreshed masters).
-- Critical failure -> dq_status FAIL (excluded from marts). Warning only -> WARN (kept, logged).
CREATE OR REPLACE TEMP TABLE _orders_dq AS
WITH clean_customers AS (SELECT DISTINCT customer_id FROM stg_customers WHERE dq_status <> 'FAIL'),
     clean_products  AS (SELECT product_id, mrp FROM stg_products WHERE dq_status <> 'FAIL'),
     clean_stores    AS (SELECT store_id FROM stg_stores WHERE dq_status <> 'FAIL')
SELECT
    o.order_id,
    fails([
        CASE WHEN o.customer_id IS NULL THEN 'ord_customer_id_not_null' END,
        CASE WHEN o.order_status = 'DELIVERED' AND o.delivery_minutes IS NULL THEN 'ord_delivered_has_minutes' END,
        CASE WHEN o.order_ts IS NULL THEN 'ord_order_ts_valid' END,
        CASE WHEN coalesce(o.order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED'), true) THEN 'ord_status_allowed' END,
        CASE WHEN coalesce(o.payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), true) THEN 'ord_payment_mode_allowed' END,
        CASE WHEN NOT coalesce(o.discount_pct BETWEEN 0 AND 100, false) THEN 'ord_discount_pct_range' END,
        CASE WHEN NOT coalesce(o.quantity > 0, false) THEN 'ord_quantity_positive' END,
        CASE WHEN NOT coalesce(abs(round(o.quantity * o.unit_price * (1 - o.discount_pct / 100), 2) - o.order_amount) <= 0.01, false)
             THEN 'ord_amount_matches_formula' END,
        CASE WHEN coalesce(o.delivery_minutes >= {{max_delivery_minutes}}, false) THEN 'ord_delivery_minutes_lt_120' END,
        CASE WHEN o.customer_id IS NOT NULL AND cc.customer_id IS NULL THEN 'ord_customer_fk' END,
        CASE WHEN cs.store_id IS NULL THEN 'ord_store_fk' END,
        CASE WHEN cp.product_id IS NULL THEN 'ord_product_fk' END,
        CASE WHEN coalesce(o.order_ts > export_cutoff(o._source_file), false) THEN 'ord_order_ts_not_future' END
    ]) AS critical_failures,
    fails([
        CASE WHEN coalesce(txt(o.raw_order_status) <> o.order_status, false) THEN 'ord_status_casing' END,
        CASE WHEN coalesce(txt(o.raw_payment_mode) <> o.payment_mode
                           AND o.payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), false)
             THEN 'ord_payment_mode_casing' END,
        CASE WHEN o.order_status = 'DELIVERED' AND coalesce(o.delivery_minutes < 8 OR o.delivery_minutes > 35, false)
             THEN 'ord_delivery_minutes_typical' END,
        CASE WHEN coalesce(o.unit_price / cp.mrp > {{price_drift_ratio}} OR o.unit_price / cp.mrp < 1 / {{price_drift_ratio}}, false)
             THEN 'ord_price_vs_mrp' END
    ]) AS warning_failures
FROM stg_orders o
LEFT JOIN clean_customers cc ON cc.customer_id = o.customer_id
LEFT JOIN clean_products  cp ON cp.product_id  = o.product_id
LEFT JOIN clean_stores    cs ON cs.store_id    = o.store_id;

UPDATE stg_orders
SET critical_failures = d.critical_failures,
    warning_failures  = d.warning_failures,
    dq_status = CASE WHEN len(d.critical_failures) > 0 THEN 'FAIL'
                     WHEN len(d.warning_failures) > 0 THEN 'WARN' ELSE 'PASS' END,
    failure_reason = array_to_string(list_concat(d.critical_failures, d.warning_failures), '; ')
FROM _orders_dq d
WHERE stg_orders.order_id = d.order_id;
