-- Write this run's quality scorecard into dq_results (one row per check in dq_check_catalog).
-- Row-level checks are counted from the failure lists on the staging tables; dataset-level checks
-- (uniqueness of order_id, freshness, volume, store capacity) are computed below.
DELETE FROM dq_results WHERE run_id = '{{run_id}}' AND dag_id = '{{dag_id}}';

INSERT INTO dq_results
WITH flags AS (
    SELECT 'orders' AS file, unnest(list_concat(critical_failures, warning_failures)) AS check_name FROM stg_orders
    UNION ALL SELECT 'payments',  unnest(list_concat(critical_failures, warning_failures)) FROM stg_payments
    UNION ALL SELECT 'customers', unnest(list_concat(critical_failures, warning_failures)) FROM stg_customers
    UNION ALL SELECT 'products',  unnest(list_concat(critical_failures, warning_failures)) FROM stg_products
    UNION ALL SELECT 'stores',    unnest(list_concat(critical_failures, warning_failures)) FROM stg_stores
),
row_counts AS (
    SELECT 'orders' AS file, count(*) AS n FROM stg_orders
    UNION ALL SELECT 'payments',  count(*) FROM stg_payments
    UNION ALL SELECT 'customers', count(*) FROM stg_customers
    UNION ALL SELECT 'products',  count(*) FROM stg_products
    UNION ALL SELECT 'stores',    count(*) FROM stg_stores
),
row_level AS (
    SELECT c.check_name, rc.n AS rows_checked, count(f.check_name) AS rows_failed
    FROM dq_check_catalog c
    JOIN row_counts rc ON rc.file = c.file
    LEFT JOIN flags f ON f.check_name = c.check_name
    WHERE c.check_name NOT IN ('ord_order_id_unique', 'ord_freshness', 'ord_volume', 'ord_store_capacity')
    GROUP BY c.check_name, rc.n
),
this_load AS (   -- rows of orders.csv loaded into raw in this run (0 when the file was unchanged)
    SELECT count(*) AS n FROM raw_orders WHERE _run_id = '{{run_id}}' AND _source_file = 'orders.csv'
),
prev_volume AS (
    SELECT rows_checked FROM dq_results
    WHERE dag_id = '{{dag_id}}' AND check_name = 'ord_volume' AND run_id <> '{{run_id}}' AND rows_checked > 0
    ORDER BY checked_at DESC LIMIT 1
),
store_days AS (
    SELECT o.store_id, o.order_date, count(*) AS n, any_value(s.capacity_orders_per_day) AS cap
    FROM stg_orders o JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status <> 'FAIL'
    WHERE o.order_date IS NOT NULL
    GROUP BY ALL
),
dataset_level AS (
    SELECT 'ord_order_id_unique' AS check_name,
           (SELECT count(*) + coalesce(sum(dup_copies), 0) FROM stg_orders) AS rows_checked,
           (SELECT coalesce(sum(dup_copies), 0) FROM stg_orders) AS rows_failed
    UNION ALL
    SELECT 'ord_freshness',
           (SELECT count(*) FROM stg_orders WHERE _source_file = 'orders.csv'),
           (SELECT CASE WHEN max(order_ts) IS NULL
                          OR max(order_ts) < export_cutoff('orders.csv') - INTERVAL 1 DAY THEN 1 ELSE 0 END
            FROM stg_orders WHERE _source_file = 'orders.csv' AND order_ts <= export_cutoff('orders.csv'))
    UNION ALL
    SELECT 'ord_volume',
           (SELECT n FROM this_load),
           (SELECT CASE WHEN (SELECT n FROM this_load) > 0
                         AND (SELECT n FROM this_load) < (SELECT rows_checked FROM prev_volume) * (1 - {{volume_drop_pct}} / 100.0)
                        THEN 1 ELSE 0 END)
    UNION ALL
    SELECT 'ord_store_capacity',
           (SELECT count(*) FROM store_days),
           (SELECT count(*) FROM store_days WHERE n > coalesce(cap, 0))
)
SELECT '{{run_id}}', '{{dag_id}}', x.check_name, c.dimension, c.severity, x.rows_checked, x.rows_failed,
       CASE WHEN x.rows_failed = 0 THEN 'PASS' WHEN c.severity = 'CRITICAL' THEN 'FAIL' ELSE 'WARN' END,
       now()
FROM (SELECT * FROM row_level UNION ALL SELECT * FROM dataset_level) x
JOIN dq_check_catalog c USING (check_name);
