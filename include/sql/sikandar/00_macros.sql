-- include/sql/sikandar/00_macros.sql
-- Helper macros shared by every ELT SQL file (created per connection, TEMP).
-- They mirror the pandas helpers in include/nammamart_sikandar/etl_rules.py.

-- trim, and treat '', N/A, NA, NULL, NONE as missing
CREATE OR REPLACE TEMP MACRO txt(x) AS
    CASE WHEN x IS NULL OR upper(trim(x)) IN ('', 'N/A', 'NA', 'NULL', 'NONE') THEN NULL ELSE trim(x) END;

CREATE OR REPLACE TEMP MACRO num(x) AS try_cast(txt(x) AS DOUBLE);

CREATE OR REPLACE TEMP MACRO ts(x) AS try_strptime(txt(x), '%Y-%m-%d %H:%M:%S');

CREATE OR REPLACE TEMP MACRO dt(x) AS try_strptime(txt(x), '%Y-%m-%d');

-- canonical payment mode; 'Cashh' is left as-is (invalid) on purpose, see SUBMISSION.md
CREATE OR REPLACE TEMP MACRO std_mode(x) AS
    CASE upper(txt(x))
        WHEN 'UPI' THEN 'UPI'
        WHEN 'CARD' THEN 'Card'
        WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
        WHEN 'COD' THEN 'Cash on Delivery'
        WHEN 'WALLET' THEN 'Wallet'
        ELSE txt(x)
    END;

-- Bronze / Silver ... (one word values)
CREATE OR REPLACE TEMP MACRO title1(x) AS upper(left(txt(x), 1)) || lower(substr(txt(x), 2));

-- export cut-off per source file: increments carry their date in the file name
CREATE OR REPLACE TEMP MACRO export_cutoff(f) AS
    CASE WHEN regexp_full_match(f, 'orders_\d{4}-\d{2}-\d{2}_increment\.csv')
         THEN strptime(substr(f, 8, 10) || ' 23:59:59', '%Y-%m-%d %H:%M:%S')
         ELSE TIMESTAMP '{{default_cutoff}}' END;

-- turn a list of CASE results into a clean list of failed check names
CREATE OR REPLACE TEMP MACRO fails(l) AS [x FOR x IN l IF x IS NOT NULL];
