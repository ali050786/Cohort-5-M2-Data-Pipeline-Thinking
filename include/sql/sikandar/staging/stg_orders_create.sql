-- stg_orders: one row per order_id (current version). Created once, then maintained by MERGE.
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id            VARCHAR,
    order_ts            TIMESTAMP,
    order_date          DATE,
    customer_id         VARCHAR,
    store_id            VARCHAR,
    product_id          VARCHAR,
    quantity            DOUBLE,
    unit_price          DOUBLE,
    discount_pct        DOUBLE,
    order_amount        DOUBLE,
    payment_mode        VARCHAR,     -- standardised
    order_status        VARCHAR,     -- standardised (upper case)
    channel             VARCHAR,
    delivery_minutes    DOUBLE,
    raw_order_ts        VARCHAR,     -- text as received, kept for validity checks / audit
    raw_order_status    VARCHAR,
    raw_payment_mode    VARCHAR,
    dup_copies          INTEGER,     -- extra copies of this order_id in the same file (quarantined)
    _source_file        VARCHAR,
    _row_num            BIGINT,
    _load_id            VARCHAR,
    _run_id             VARCHAR,
    _merged_at          TIMESTAMP,
    critical_failures   VARCHAR[],
    warning_failures    VARCHAR[],
    dq_status           VARCHAR,
    failure_reason      VARCHAR
);

-- every status change made by a later file is kept, so September can be reported "as first reported"
CREATE TABLE IF NOT EXISTS stg_orders_status_history (
    order_id        VARCHAR,
    old_status      VARCHAR,
    new_status      VARCHAR,
    old_order_amount DOUBLE,
    changed_by_file VARCHAR,
    run_id          VARCHAR,
    changed_at      TIMESTAMP
);
