-- MERGE one raw load of orders (orders.csv or an orders_<date>_increment.csv) into stg_orders.
-- Upsert on order_id: corrections (e.g. DELIVERED -> RETURNED) overwrite the row, new orders are added.
-- A row is only overwritten by a file whose export date is the same or newer.
-- Parameter: {{load_id}}
CREATE OR REPLACE TEMP TABLE _orders_src AS
WITH src AS (
    SELECT r.*,
           row_number() OVER (PARTITION BY order_id ORDER BY _row_num) AS rn,
           count(*)     OVER (PARTITION BY order_id)                   AS copies
    FROM raw_orders r
    WHERE _load_id = '{{load_id}}'
)
SELECT
    order_id,
    ts(order_ts)                         AS order_ts,
    CAST(ts(order_ts) AS DATE)           AS order_date,
    txt(customer_id)                     AS customer_id,
    txt(store_id)                        AS store_id,
    txt(product_id)                      AS product_id,
    num(quantity)                        AS quantity,
    num(unit_price)                      AS unit_price,
    num(discount_pct)                    AS discount_pct,
    num(order_amount)                    AS order_amount,
    std_mode(payment_mode)               AS payment_mode,
    upper(txt(order_status))             AS order_status,
    txt(channel)                         AS channel,
    num(delivery_minutes)                AS delivery_minutes,
    order_ts                             AS raw_order_ts,
    order_status                         AS raw_order_status,
    payment_mode                         AS raw_payment_mode,
    CAST(copies - 1 AS INTEGER)          AS dup_copies,
    _source_file, _row_num, _load_id, _run_id,
    now()                                AS _merged_at,
    CAST(NULL AS VARCHAR[])              AS critical_failures,
    CAST(NULL AS VARCHAR[])              AS warning_failures,
    CAST(NULL AS VARCHAR)                AS dq_status,
    CAST(NULL AS VARCHAR)                AS failure_reason
FROM src
WHERE rn = 1;          -- duplicate copies stay in raw_orders and are reported as quarantined

INSERT INTO stg_orders_status_history
SELECT t.order_id, t.order_status, s.order_status, t.order_amount, s._source_file, s._run_id, now()
FROM _orders_src s JOIN stg_orders t USING (order_id)
WHERE t.order_status IS DISTINCT FROM s.order_status
  AND export_cutoff(s._source_file) >= export_cutoff(t._source_file);

MERGE INTO stg_orders t
USING _orders_src s
ON t.order_id = s.order_id
-- never let an older export (e.g. a re-sent September file) overwrite a newer correction
WHEN MATCHED AND export_cutoff(s._source_file) >= export_cutoff(t._source_file) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
