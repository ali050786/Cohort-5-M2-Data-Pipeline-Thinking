-- Business-ready order view used by every mart: only rows without a critical failure,
-- joined to clean dimensions, with the derived measures the client asked for.
CREATE OR REPLACE VIEW mart_orders_clean AS
SELECT
    o.order_id, o.order_ts, o.order_date, o.customer_id, o.store_id, o.product_id,
    CAST(o.quantity AS INTEGER)                                   AS quantity,
    o.unit_price, o.discount_pct, o.order_amount, o.payment_mode, o.order_status, o.channel,
    o.delivery_minutes,
    s.store_name, s.zone, p.category, p.cost_price,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END                         AS net_revenue,
    CASE WHEN o.order_status = 'DELIVERED' THEN round(o.quantity * p.cost_price, 2) ELSE 0 END     AS cost_amount,
    CASE WHEN o.order_status = 'DELIVERED'
         THEN round(o.order_amount - round(o.quantity * p.cost_price, 2), 2) ELSE 0 END           AS gross_margin,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes <= {{sla_minutes}} END         AS within_15_min,
    o.dq_status, o._source_file, o._row_num, o._load_id
FROM stg_orders o
JOIN stg_stores   s ON s.store_id   = o.store_id   AND s.dq_status <> 'FAIL'
JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status <> 'FAIL'
WHERE o.dq_status <> 'FAIL';
