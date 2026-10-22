-- stg_payments: latest payments.csv load -> typed, standardised, quality-flagged.
-- Duplicate payment_id copies are kept (flagged FAIL) so reconciliation can list them.
CREATE OR REPLACE TABLE stg_payments AS
WITH src AS (
    SELECT r.*, row_number() OVER (PARTITION BY payment_id ORDER BY _row_num) AS rn
    FROM raw_payments r
    WHERE _load_id = (SELECT load_id FROM raw_file_manifest WHERE source_file = 'payments.csv'
                      ORDER BY loaded_at DESC LIMIT 1)
),
typed AS (
    SELECT
        s.payment_id,
        txt(s.order_id)                        AS order_id,
        ts(s.payment_ts)                       AS payment_ts,
        std_mode(s.payment_mode)               AS payment_mode,
        num(s.amount_paid)                     AS amount_paid,
        upper(txt(s.payment_status))           AS payment_status,
        txt(s.gateway_ref)                     AS gateway_ref,
        s.rn > 1                               AS is_duplicate_copy,
        fails([
            CASE WHEN s.rn > 1 THEN 'pay_payment_id_unique' END,
            CASE WHEN txt(s.order_id) IS NULL THEN 'pay_order_id_not_null' END,
            CASE WHEN ts(s.payment_ts) IS NULL THEN 'pay_payment_ts_valid' END,
            CASE WHEN coalesce(upper(txt(s.payment_status)) NOT IN ('SUCCESS', 'FAILED', 'REFUNDED'), true) THEN 'pay_status_allowed' END,
            CASE WHEN coalesce(std_mode(s.payment_mode) NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), true) THEN 'pay_mode_allowed' END,
            CASE WHEN upper(txt(s.payment_status)) = 'SUCCESS' AND NOT coalesce(num(s.amount_paid) > 0, false)
                 THEN 'pay_success_amount_positive' END,
            CASE WHEN o.order_id IS NULL THEN 'pay_order_fk' END,
            CASE WHEN coalesce(ts(s.payment_ts) > export_cutoff('payments.csv'), false) THEN 'pay_payment_ts_not_future' END
        ])                                     AS critical_failures,
        fails([
            CASE WHEN upper(txt(s.payment_status)) = 'SUCCESS' AND o.order_amount IS NOT NULL
                      AND coalesce(abs(num(s.amount_paid) - o.order_amount) > 0.01, false)
                 THEN 'pay_amount_matches_order' END,
            CASE WHEN coalesce(std_mode(s.payment_mode) <> 'Cash on Delivery', false) AND txt(s.gateway_ref) IS NULL
                 THEN 'pay_gateway_ref_present' END,
            CASE WHEN coalesce(txt(s.payment_mode) <> std_mode(s.payment_mode)
                               AND std_mode(s.payment_mode) IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), false)
                 THEN 'pay_mode_casing' END
        ])                                     AS warning_failures,
        s._source_file, s._row_num, s._load_id, s._run_id
    FROM src s
    LEFT JOIN stg_orders o ON o.order_id = s.order_id
)
SELECT *,
       CASE WHEN len(critical_failures) > 0 THEN 'FAIL'
            WHEN len(warning_failures) > 0 THEN 'WARN' ELSE 'PASS' END     AS dq_status,
       array_to_string(list_concat(critical_failures, warning_failures), '; ') AS failure_reason
FROM typed;
