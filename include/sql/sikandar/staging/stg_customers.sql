-- stg_customers: latest customers.csv load -> typed, standardised, PII masked, quality-flagged.
-- Rebuilt in full each run (the CRM export is a full snapshot).
CREATE OR REPLACE TABLE stg_customers AS
WITH src AS (
    SELECT r.*,
           row_number() OVER (PARTITION BY txt(customer_id) ORDER BY _row_num) AS rn
    FROM raw_customers r
    WHERE _load_id = (SELECT load_id FROM raw_file_manifest WHERE source_file = 'customers.csv'
                      ORDER BY loaded_at DESC LIMIT 1)
),
typed AS (
    SELECT
        txt(customer_id)                       AS customer_id,
        -- personal data: masked here, the raw value never leaves raw_customers
        CASE WHEN txt(full_name) IS NULL THEN NULL
             WHEN strpos(txt(full_name), ' ') = 0 THEN txt(full_name)
             ELSE split_part(txt(full_name), ' ', 1) || ' ' || left(string_split(txt(full_name), ' ')[-1], 1) || '.'
        END                                    AS customer_name_masked,
        CASE WHEN txt(email) IS NULL THEN NULL
             WHEN strpos(txt(email), '@') > 0
                  THEN left(split_part(txt(email), '@', 1), 2) || '***@' || substr(txt(email), strpos(txt(email), '@') + 1)
             ELSE left(txt(email), 2) || '***'
        END                                    AS email_masked,
        CASE WHEN txt(phone) IS NULL THEN NULL
             WHEN length(regexp_replace(phone, '\D', '', 'g')) <= 4 THEN repeat('*', length(regexp_replace(phone, '\D', '', 'g')))
             ELSE left(regexp_replace(phone, '\D', '', 'g'), 2)
                  || repeat('*', length(regexp_replace(phone, '\D', '', 'g')) - 4)
                  || right(regexp_replace(phone, '\D', '', 'g'), 2)
        END                                    AS phone_masked,
        txt(city)                              AS city,
        txt(area)                              AS area,
        txt(pincode)                           AS pincode,
        CAST(dt(signup_date) AS DATE)          AS signup_date,
        title1(loyalty_tier)                   AS loyalty_tier,
        txt(is_active) = 'Y'                   AS is_active,
        fails([
            CASE WHEN txt(customer_id) IS NULL THEN 'cust_customer_id_not_null' END,
            CASE WHEN rn > 1 AND txt(customer_id) IS NOT NULL THEN 'cust_customer_id_unique' END,
            CASE WHEN coalesce(title1(loyalty_tier) NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum'), true)
                 THEN 'cust_loyalty_tier_allowed' END
        ])                                     AS critical_failures,
        fails([
            CASE WHEN NOT coalesce(regexp_full_match(txt(email), '[^@\s]+@[^@\s]+\.[A-Za-z]{2,}'), false) THEN 'cust_email_format' END,
            CASE WHEN NOT coalesce(regexp_full_match(txt(phone), '[6-9][0-9]{9}'), false) THEN 'cust_phone_10_digits' END,
            CASE WHEN NOT coalesce(regexp_full_match(txt(pincode), '[0-9]{6}'), false) THEN 'cust_pincode_6_digits' END,
            CASE WHEN txt(city) IS NULL THEN 'cust_city_present' END,
            CASE WHEN coalesce(txt(loyalty_tier) <> title1(loyalty_tier), false) THEN 'cust_loyalty_tier_casing' END,
            CASE WHEN coalesce(dt(signup_date) > export_cutoff('customers.csv'), false) THEN 'cust_signup_not_future' END
        ])                                     AS warning_failures,
        _source_file, _row_num, _load_id, _run_id
    FROM src
)
SELECT *,
       CASE WHEN len(critical_failures) > 0 THEN 'FAIL'
            WHEN len(warning_failures) > 0 THEN 'WARN' ELSE 'PASS' END     AS dq_status,
       array_to_string(list_concat(critical_failures, warning_failures), '; ') AS failure_reason
FROM typed;
