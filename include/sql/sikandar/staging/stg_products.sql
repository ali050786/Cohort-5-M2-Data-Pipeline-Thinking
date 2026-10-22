-- stg_products: latest products.csv load -> typed, standardised, quality-flagged.
CREATE OR REPLACE TABLE stg_products AS
WITH src AS (
    SELECT r.*, row_number() OVER (PARTITION BY txt(product_id) ORDER BY _row_num) AS rn
    FROM raw_products r
    WHERE _load_id = (SELECT load_id FROM raw_file_manifest WHERE source_file = 'products.csv'
                      ORDER BY loaded_at DESC LIMIT 1)
),
typed AS (
    SELECT
        txt(product_id)                        AS product_id,
        txt(product_name)                      AS product_name,
        coalesce(txt(category), 'Unknown')     AS category,
        txt(sub_category)                      AS sub_category,
        txt(brand)                             AS brand,
        num(mrp)                               AS mrp,
        num(cost_price)                        AS cost_price,
        txt(unit)                              AS unit,
        txt(is_perishable) = 'True'            AS is_perishable,
        CAST(dt(launch_date) AS DATE)          AS launch_date,
        fails([
            CASE WHEN rn > 1 THEN 'prod_product_id_unique' END,
            CASE WHEN NOT coalesce(num(mrp) > 0, false) THEN 'prod_mrp_positive' END,
            CASE WHEN NOT coalesce(num(cost_price) <= num(mrp), false) THEN 'prod_cost_le_mrp' END,
            CASE WHEN coalesce(dt(launch_date) > export_cutoff('products.csv'), false) THEN 'prod_launch_not_future' END
        ])                                     AS critical_failures,
        fails([
            CASE WHEN txt(category) IS NULL THEN 'prod_category_present' END,
            CASE WHEN coalesce(txt(is_perishable) NOT IN ('True', 'False'), true) THEN 'prod_is_perishable_valid' END,
            CASE WHEN coalesce(contains(txt(product_name), '(old)'), false) THEN 'prod_consistent_cost_by_name' END
        ])                                     AS warning_failures,
        _source_file, _row_num, _load_id, _run_id
    FROM src
)
SELECT *,
       CASE WHEN len(critical_failures) > 0 THEN 'FAIL'
            WHEN len(warning_failures) > 0 THEN 'WARN' ELSE 'PASS' END     AS dq_status,
       array_to_string(list_concat(critical_failures, warning_failures), '; ') AS failure_reason
FROM typed;
