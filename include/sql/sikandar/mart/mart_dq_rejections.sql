-- Q6: rows rejected (critical failure) by file and by reason, in the current staging state.
-- Duplicate copies of an order_id never reach stg_orders, so they are counted from raw_orders.
CREATE OR REPLACE TABLE mart_dq_rejections AS
WITH rejected AS (
    SELECT _source_file AS source_file, unnest(critical_failures) AS reason FROM stg_orders WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, unnest(critical_failures) FROM stg_payments  WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, unnest(critical_failures) FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, unnest(critical_failures) FROM stg_products  WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, unnest(critical_failures) FROM stg_stores    WHERE dq_status = 'FAIL'
    UNION ALL SELECT _source_file, 'ord_order_id_unique' FROM stg_orders, range(dup_copies) WHERE dup_copies > 0
)
SELECT source_file, reason, count(*) AS rows_rejected, '{{run_id}}' AS run_id
FROM rejected GROUP BY ALL ORDER BY source_file, rows_rejected DESC;
