-- Q4 (part): one row per order/payment mismatch, with a reason.
-- difference = what the gateway shows - what the order book expects.
-- Same logic as include/nammamart_sikandar/reconcile.py (ETL).
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH orders AS (
    SELECT order_id, order_status, order_amount, payment_mode AS order_mode
    FROM stg_orders WHERE dq_status <> 'FAIL'
),
export_ids AS (SELECT order_id FROM stg_orders),
pays AS (
    SELECT payment_id, order_id, payment_status, payment_mode AS pay_mode, amount_paid
    FROM stg_payments WHERE NOT is_duplicate_copy
),
dups AS (
    SELECT payment_id, order_id, payment_status, amount_paid
    FROM stg_payments WHERE is_duplicate_copy
),
m AS (
    SELECT coalesce(o.order_id, p.order_id) AS order_id, p.payment_id, o.order_status, o.order_amount,
           p.payment_status, p.amount_paid, o.order_mode, p.pay_mode,
           o.order_id IS NOT NULL AS has_order, p.payment_id IS NOT NULL AS has_payment
    FROM orders o FULL OUTER JOIN pays p ON o.order_id = p.order_id
)
SELECT order_id, payment_id, NULL::VARCHAR AS order_status, NULL::DOUBLE AS order_amount, payment_status,
       amount_paid, round(amount_paid, 2) AS difference, 'DUPLICATE_PAYMENT' AS mismatch_reason
FROM dups
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       round(amount_paid, 2), 'PAYMENT_WITHOUT_ORDER'
FROM m WHERE NOT has_order AND order_id NOT IN (SELECT order_id FROM export_ids)
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       round(amount_paid, 2), 'PAYMENT_FOR_REJECTED_ORDER'
FROM m WHERE NOT has_order AND order_id IN (SELECT order_id FROM export_ids)
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       round(amount_paid - order_amount, 2), 'AMOUNT_MISMATCH'
FROM m WHERE has_order AND payment_status = 'SUCCESS' AND abs(amount_paid - order_amount) > 0.01
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       0, 'PAYMENT_MODE_MISMATCH'
FROM m WHERE has_order AND has_payment AND pay_mode IS DISTINCT FROM order_mode
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       round(CASE WHEN payment_status = 'SUCCESS' THEN amount_paid ELSE 0 END - order_amount, 2), 'DELIVERED_NOT_PAID'
FROM m WHERE has_order AND order_status = 'DELIVERED' AND payment_status IS DISTINCT FROM 'SUCCESS'
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       round(CASE WHEN payment_status = 'SUCCESS' THEN amount_paid ELSE 0 END, 2), 'RETURNED_NOT_REFUNDED'
FROM m WHERE has_order AND order_status = 'RETURNED' AND payment_status IS DISTINCT FROM 'REFUNDED'
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       round(-amount_paid, 2), 'REFUND_WITHOUT_RETURN'
FROM m WHERE has_order AND payment_status = 'REFUNDED' AND order_status <> 'RETURNED'
UNION ALL
SELECT order_id, payment_id, order_status, order_amount, payment_status, amount_paid,
       round(amount_paid, 2), 'PAID_BUT_CANCELLED'
FROM m WHERE has_order AND order_status = 'CANCELLED' AND payment_status = 'SUCCESS'
ORDER BY mismatch_reason, order_id;
