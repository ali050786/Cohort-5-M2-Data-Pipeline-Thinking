-- stg_stores: latest stores.csv load -> typed, standardised, quality-flagged.
CREATE OR REPLACE TABLE stg_stores AS
WITH src AS (
    SELECT r.*, row_number() OVER (PARTITION BY txt(store_id) ORDER BY _row_num) AS rn
    FROM raw_stores r
    WHERE _load_id = (SELECT load_id FROM raw_file_manifest WHERE source_file = 'stores.csv'
                      ORDER BY loaded_at DESC LIMIT 1)
),
typed AS (
    SELECT
        txt(store_id)                          AS store_id,
        txt(store_name)                        AS store_name,
        coalesce(txt(zone), 'Unknown')         AS zone,
        txt(area)                              AS area,
        txt(pincode)                           AS pincode,
        CAST(dt(open_date) AS DATE)            AS open_date,
        txt(manager_name)                      AS manager_name,
        CAST(num(capacity_orders_per_day) AS INTEGER) AS capacity_orders_per_day,
        fails([
            CASE WHEN rn > 1 THEN 'store_store_id_unique' END,
            CASE WHEN coalesce(dt(open_date) > export_cutoff('stores.csv'), false) THEN 'store_open_date_not_future' END
        ])                                     AS critical_failures,
        fails([
            CASE WHEN txt(zone) IS NULL THEN 'store_zone_present' END,
            CASE WHEN coalesce(txt(zone) NOT IN ('North', 'South', 'East', 'West'), true) THEN 'store_zone_allowed' END,
            CASE WHEN NOT coalesce(regexp_full_match(txt(pincode), '[0-9]{6}'), false) THEN 'store_pincode_6_digits' END,
            CASE WHEN NOT coalesce(num(capacity_orders_per_day) > 0, false) THEN 'store_capacity_positive' END,
            CASE WHEN NOT coalesce(txt(store_name) = 'NammaMart ' || txt(area), false) THEN 'store_consistent_area' END
        ])                                     AS warning_failures,
        _source_file, _row_num, _load_id, _run_id
    FROM src
)
SELECT *,
       CASE WHEN len(critical_failures) > 0 THEN 'FAIL'
            WHEN len(warning_failures) > 0 THEN 'WARN' ELSE 'PASS' END     AS dq_status,
       array_to_string(list_concat(critical_failures, warning_failures), '; ') AS failure_reason
FROM typed;
