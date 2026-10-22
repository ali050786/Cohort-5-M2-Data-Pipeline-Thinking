"""
dags/nammamart_etl_sikandar.py
------------------------------
NammaMart Quick Commerce: ETL pipeline

Extract -> Validate -> Quarantine -> Transform -> Load -> Reconcile -> Report -> Audit

  extract_orders ────┐
  extract_payments ──┤        validate_customers ─┐
  extract_customers ─┼──────► validate_products ──┼─► validate_orders ─► validate_payments
  extract_products ──┤        validate_stores ────┘          │                 │
  extract_stores ────┘                                       ▼                 ▼
                     record_dq_results ◄──── quarantine_failed_rows ◄──────────┘
                                                     │
                                  transform ─► load_star_schema ─► reconcile_payments ─► build_reports
                                                                                              │
                                                                     run_audit (all_done) ◄───┘

* Pandas does the cleaning BEFORE anything reaches the warehouse: only rows that pass every
  critical check land in etl_fact_orders (the finance table).
* Failed rows go to include/quarantine/sikandar/<file>_failed_<run_id>.csv with failure_reason,
  and to the etl_rejections table.
* Loading is idempotent: dimensions are replaced as a snapshot, the fact is MERGEd on order_id.
* Any orders_<date>_increment.csv found next to orders.csv is applied after it (newer file wins),
  so new orders are appended and late status changes (e.g. RETURNED) overwrite the old row.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from airflow.exceptions import AirflowFailException  # noqa: E402
from airflow.sdk import Param, dag, get_current_context, task  # noqa: E402

from nammamart_sikandar import common as C  # noqa: E402

DAG_ID = "nammamart_etl_sikandar"

default_args = {
    "owner": "sikandar",
    "retries": 2,                                   # a late file gets two more chances
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),     # nothing here should take > 10 min
    "on_failure_callback": C.slack_on_failure,      # Slack via Variable SLACK_WEBHOOK_URL
}


def _ctx():
    ctx = get_current_context()
    params = ctx["params"]
    data_dir = C.resolve(params.get("data_dir"), C.DEFAULT_DATA_DIR)
    wh = C.resolve(params.get("warehouse"), C.DEFAULT_WAREHOUSE)
    run_id = ctx["run_id"]
    tmp = C.TMP_DIR / DAG_ID / C.safe_run_id(run_id)
    tmp.mkdir(parents=True, exist_ok=True)
    return ctx, data_dir, wh, run_id, tmp


def _read_source(file_key: str):
    """Read a source file as text, exactly as received; fail loudly on schema change."""
    import pandas as pd

    ctx, data_dir, _, run_id, tmp = _ctx()
    files = [data_dir / f"{file_key}.csv"]
    if file_key == "orders":
        files += sorted(data_dir.glob(C.INCREMENT_GLOB))
    frames = []
    for seq, f in enumerate(files):
        if not f.exists():
            raise FileNotFoundError(f"[Extract] source file missing: {f}")   # retried
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        expected = C.SCHEMAS[file_key]
        if list(df.columns) != expected:
            missing = sorted(set(expected) - set(df.columns))
            extra = sorted(set(df.columns) - set(expected))
            raise AirflowFailException(                                       # not retried
                f"[Schema] {f.name}: columns changed. missing={missing} unexpected={extra}")
        df["_source_file"] = f.name
        df["_file_seq"] = seq
        df["_row_num"] = range(1, len(df) + 1)
        frames.append(df)
        print(f"[Extract] {f.name}: {len(df)} rows")
    out = pd.concat(frames, ignore_index=True)
    path = tmp / f"extract_{file_key}.pkl"
    out.to_pickle(path)
    return {"file": file_key, "path": str(path), "rows": len(out), "files": [f.name for f in files]}


@dag(
    dag_id=DAG_ID,
    description="NammaMart ETL: validate in pandas, load only clean rows into a DuckDB star schema",
    # 09:00 IST, Monday to Saturday. The timezone comes from start_date (Asia/Kolkata).
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,          # the report is about "today"; replaying missed days is a manual decision
    max_active_runs=1,      # one writer at a time on the warehouse file
    default_args=default_args,
    params={
        "data_dir": Param("data/nammamart", type="string",
                          description="Source folder, relative to include/ (use a copy for experiments)"),
        "warehouse": Param("warehouse_sikandar.duckdb", type="string",
                           description="DuckDB file, relative to include/"),
    },
    tags=["nammamart", "etl", "sikandar", "module2"],
)
def nammamart_etl_sikandar():

    # ── EXTRACT (five in parallel) ───────────────────────────────────────────
    @task
    def extract_orders():
        return _read_source("orders")

    @task
    def extract_payments():
        return _read_source("payments")

    @task
    def extract_customers():
        return _read_source("customers")

    @task
    def extract_products():
        return _read_source("products")

    @task
    def extract_stores():
        return _read_source("stores")

    # ── VALIDATE ─────────────────────────────────────────────────────────────
    def _validate_dim(meta, fn, name):
        import pandas as pd
        *_, tmp = _ctx()
        df, results = fn(pd.read_pickle(meta["path"]))
        path = tmp / f"validated_{name}.pkl"
        df.to_pickle(path)
        failed = int((df.dq_status == "FAIL").sum())
        print(f"[Validate] {name}: {len(df)} rows, {len(df) - failed} passed, {failed} failed")
        for r in results:
            if r["rows_failed"]:
                print(f"    {r['check_name']:32s} {C.CHECK_META[r['check_name']]['severity']:8s} failed={r['rows_failed']}")
        return {"file": name, "path": str(path), "rows": len(df), "failed": failed, "results": results}

    @task
    def validate_customers(meta):
        from nammamart_sikandar.etl_rules import validate_customers as v
        return _validate_dim(meta, v, "customers")

    @task
    def validate_products(meta):
        from nammamart_sikandar.etl_rules import validate_products as v
        return _validate_dim(meta, v, "products")

    @task
    def validate_stores(meta):
        from nammamart_sikandar.etl_rules import validate_stores as v
        return _validate_dim(meta, v, "stores")

    @task
    def validate_orders(meta, cust, prod, stores):
        import pandas as pd
        from nammamart_sikandar.etl_rules import num, validate_orders as v

        _, _, wh, run_id, tmp = _ctx()
        c = pd.read_pickle(cust["path"])
        p = pd.read_pickle(prod["path"])
        s = pd.read_pickle(stores["path"])
        clean_c = c.loc[c.dq_status != "FAIL", "customer_id_c"]
        clean_p = p.loc[p.dq_status != "FAIL", ["product_id_c", "mrp_c"]].rename(
            columns={"product_id_c": "product_id", "mrp_c": "mrp"})
        clean_s = s.loc[s.dq_status != "FAIL", ["store_id_c", "capacity_orders_per_day"]].rename(
            columns={"store_id_c": "store_id"})
        clean_s["capacity_orders_per_day"] = num(clean_s.capacity_orders_per_day)

        prev = None
        if wh.exists():
            con = C.connect(wh)
            prev = C.previous_orders_volume(con, DAG_ID, run_id)
            con.close()
        df, results, info = v(pd.read_pickle(meta["path"]), clean_c, clean_p, clean_s, prev)
        path = tmp / "validated_orders.pkl"
        df.to_pickle(path)
        failed = int((df.dq_status == "FAIL").sum())
        print(f"[Validate] orders: {len(df)} rows, {len(df) - failed} passed, {failed} failed | {info}")
        for r in results:
            if r["rows_failed"]:
                print(f"    {r['check_name']:32s} {C.CHECK_META[r['check_name']]['severity']:8s} failed={r['rows_failed']}")
        return {"file": "orders", "path": str(path), "rows": len(df), "failed": failed, "results": results}

    @task
    def validate_payments(meta, orders_v):
        import pandas as pd
        from nammamart_sikandar.etl_rules import validate_payments as v

        *_, tmp = _ctx()
        o = pd.read_pickle(orders_v["path"])
        latest = o.sort_values(["_file_seq", "_row_num"]).drop_duplicates("order_id", keep="last")
        df, results = v(pd.read_pickle(meta["path"]), set(o.order_id), latest.set_index("order_id").order_amount_c)
        path = tmp / "validated_payments.pkl"
        df.to_pickle(path)
        failed = int((df.dq_status == "FAIL").sum())
        print(f"[Validate] payments: {len(df)} rows, {len(df) - failed} passed, {failed} failed")
        for r in results:
            if r["rows_failed"]:
                print(f"    {r['check_name']:32s} {C.CHECK_META[r['check_name']]['severity']:8s} failed={r['rows_failed']}")
        return {"file": "payments", "path": str(path), "rows": len(df), "failed": failed, "results": results}

    # ── DQ SCORECARD ─────────────────────────────────────────────────────────
    @task
    def record_dq_results(*validated):
        _, _, wh, run_id, _ = _ctx()
        results = [r for v in validated for r in v["results"]]
        con = C.connect(wh)
        C.write_dq_results(con, run_id, DAG_ID, results)
        summary = con.execute(
            "SELECT status, count(*) FROM dq_results WHERE run_id=? AND dag_id=? GROUP BY 1 ORDER BY 1",
            [run_id, DAG_ID]).fetchall()
        con.close()
        print(f"[DQ] {len(results)} checks recorded: {summary}")
        return len(results)

    # ── QUARANTINE ───────────────────────────────────────────────────────────
    @task
    def quarantine_failed_rows(*validated):
        import json
        import pandas as pd

        _, _, wh, run_id, _ = _ctx()
        qdir = C.QUARANTINE_DIR
        qdir.mkdir(parents=True, exist_ok=True)
        rid = C.safe_run_id(run_id)
        rejections, total = [], 0
        for v in validated:
            df = pd.read_pickle(v["path"])
            bad = df[df.dq_status == "FAIL"]
            raw_cols = C.SCHEMAS[v["file"]]
            if bad.empty:
                print(f"[Quarantine] {v['file']}: no failed rows")
                continue
            out = bad[raw_cols + ["_source_file", "_row_num"]].copy()
            out["failure_reason"] = bad.critical_failures
            out["warnings"] = bad.warning_failures
            path = qdir / f"{v['file']}_failed_{rid}.csv"
            out.to_csv(path, index=False)
            total += len(out)
            print(f"[Quarantine] {v['file']}: {len(out)} rows -> {path.name}")
            for _, r in bad.iterrows():
                rejections.append([run_id, DAG_ID, r["_source_file"], int(r["_row_num"]), r[raw_cols[0]],
                                   r["critical_failures"], json.dumps({k: r[k] for k in raw_cols}), datetime.now()])
        con = C.connect(wh)
        con.execute("""CREATE TABLE IF NOT EXISTS etl_rejections (
            run_id VARCHAR, dag_id VARCHAR, source_file VARCHAR, source_row_num INTEGER, row_key VARCHAR,
            failure_reason VARCHAR, raw_record JSON, rejected_at TIMESTAMP)""")
        con.execute("DELETE FROM etl_rejections WHERE run_id = ?", [run_id])
        if rejections:
            con.executemany("INSERT INTO etl_rejections VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rejections)
        con.close()
        return total

    # ── TRANSFORM ────────────────────────────────────────────────────────────
    @task
    def transform(orders_v, payments_v, cust_v, prod_v, stores_v):
        import hashlib
        import pandas as pd
        from nammamart_sikandar.etl_rules import txt, num, ts

        *_, run_id, tmp = _ctx()
        now = datetime.now()

        # dimensions: keep only rows that passed critical checks; standardise; mask PII
        c = pd.read_pickle(cust_v["path"])
        c = c[c.dq_status != "FAIL"]
        dim_customer = pd.DataFrame({
            "customer_id": c.customer_id_c,
            "customer_name_masked": txt(c.full_name).map(C.mask_name),
            "email_masked": c.email_c.map(C.mask_email),
            "phone_masked": c.phone_c.map(C.mask_phone),
            "city": txt(c.city),
            "area": txt(c.area),
            "pincode": c.pincode_c,
            "signup_date": c.signup_date_c.dt.date,
            "loyalty_tier": c.loyalty_tier_c,
            "is_active": txt(c.is_active) == "Y",
            "dq_warnings": c.warning_failures,
        })

        p = pd.read_pickle(prod_v["path"])
        p = p[p.dq_status != "FAIL"]
        dim_product = pd.DataFrame({
            "product_id": p.product_id_c,
            "product_name": txt(p.product_name),
            "category": txt(p.category).fillna("Unknown"),
            "sub_category": txt(p.sub_category),
            "brand": txt(p.brand),
            "mrp": p.mrp_c,
            "cost_price": p.cost_price_c,
            "unit": txt(p.unit),
            "is_perishable": txt(p.is_perishable) == "True",
            "launch_date": ts(p.launch_date, "%Y-%m-%d").dt.date,
            "dq_warnings": p.warning_failures,
        })

        s = pd.read_pickle(stores_v["path"])
        s = s[s.dq_status != "FAIL"]
        dim_store = pd.DataFrame({
            "store_id": s.store_id_c,
            "store_name": txt(s.store_name),
            "zone": txt(s.zone).fillna("Unknown"),
            "area": txt(s.area),
            "pincode": txt(s.pincode),
            "open_date": ts(s.open_date, "%Y-%m-%d").dt.date,
            "manager_name": txt(s.manager_name),
            "capacity_orders_per_day": num(s.capacity_orders_per_day).astype("Int64"),
            "dq_warnings": s.warning_failures,
        })

        # fact: clean orders only; the newest file wins for the same order_id
        o = pd.read_pickle(orders_v["path"])
        o = o[o.dq_status != "FAIL"].sort_values(["_file_seq", "_row_num"]).drop_duplicates("order_id", keep="last")
        o = o.merge(dim_product[["product_id", "cost_price"]], left_on="product_id_c", right_on="product_id",
                    how="left", suffixes=("", "_p"))
        delivered = o.order_status_c == "DELIVERED"
        fact = pd.DataFrame({
            "order_id": o.order_id,
            "order_ts": o.order_ts_c,
            "order_date": o.order_ts_c.dt.date,
            "customer_id": o.customer_id_c,
            "store_id": o.store_id_c,
            "product_id": o.product_id_c,
            "quantity": o.quantity_c.astype("Int64"),
            "unit_price": o.unit_price_c,
            "discount_pct": o.discount_pct_c,
            "order_amount": o.order_amount_c,
            "payment_mode": o.payment_mode_c,
            "order_status": o.order_status_c,
            "channel": o.channel_c,
            "delivery_minutes": o.delivery_minutes_c,
            "cost_price": o.cost_price,
            "net_revenue": o.order_amount_c.where(delivered, 0.0).round(2),
            "cost_amount": (o.quantity_c * o.cost_price).where(delivered, 0.0).round(2),
        })
        fact["gross_margin"] = (fact.net_revenue - fact.cost_amount).round(2)
        fact["within_15_min"] = (delivered & (o.delivery_minutes_c <= C.SLA_MINUTES)).where(delivered)
        fact["dq_status"] = o.dq_status.values
        fact["dq_warnings"] = o.warning_failures.values
        fact["source_file"] = o._source_file.values
        fact["source_row_num"] = o._row_num.values
        business_cols = [c for c in fact.columns if c not in ("source_file", "source_row_num")]
        fact["row_hash"] = fact[business_cols].astype(str).agg("|".join, axis=1).map(
            lambda x: hashlib.md5(x.encode()).hexdigest())
        fact["run_id"] = run_id
        fact["loaded_at"] = now

        out = {}
        for name, df in {"dim_customer": dim_customer, "dim_product": dim_product,
                         "dim_store": dim_store, "fact_orders": fact}.items():
            path = tmp / f"transformed_{name}.pkl"
            df.to_pickle(path)
            out[name] = str(path)
            print(f"[Transform] {name}: {len(df)} rows")
        print(f"[Transform] PII masked, e.g. {dim_customer.email_masked.iloc[0]} / {dim_customer.phone_masked.iloc[0]}")
        out["fact_rows"] = len(fact)
        out["payments_path"] = payments_v["path"]
        out["orders_path"] = orders_v["path"]
        return out

    # ── LOAD (idempotent) ────────────────────────────────────────────────────
    @task
    def load_star_schema(t):
        import pandas as pd

        _, _, wh, run_id, _ = _ctx()
        con = C.connect(wh)
        con.execute("BEGIN TRANSACTION")
        # dimensions are full snapshots -> replace their content
        for name in ("dim_customer", "dim_product", "dim_store"):
            df = pd.read_pickle(t[name])  # noqa: F841  (used by DuckDB replacement scan)
            con.execute(f"CREATE TABLE IF NOT EXISTS etl_{name} AS SELECT * FROM df WHERE false")
            con.execute(f"DELETE FROM etl_{name}")
            con.execute(f"INSERT INTO etl_{name} BY NAME SELECT * FROM df")

        stage = pd.read_pickle(t["fact_orders"])  # noqa: F841
        con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM stage WHERE false")
        con.execute("""CREATE TABLE IF NOT EXISTS etl_order_status_history (
            order_id VARCHAR, old_status VARCHAR, new_status VARCHAR, old_net_revenue DOUBLE,
            new_net_revenue DOUBLE, changed_by_file VARCHAR, run_id VARCHAR, changed_at TIMESTAMP)""")
        inserted = con.execute(
            "SELECT count(*) FROM stage s ANTI JOIN etl_fact_orders f USING (order_id)").fetchone()[0]
        updated = con.execute(
            "SELECT count(*) FROM stage s JOIN etl_fact_orders f USING (order_id) WHERE s.row_hash <> f.row_hash"
        ).fetchone()[0]
        con.execute("""
            INSERT INTO etl_order_status_history
            SELECT f.order_id, f.order_status, s.order_status, f.net_revenue, s.net_revenue,
                   s.source_file, ?, now()
            FROM stage s JOIN etl_fact_orders f USING (order_id)
            WHERE s.order_status <> f.order_status""", [run_id])
        con.execute("""
            MERGE INTO etl_fact_orders f USING stage s ON f.order_id = s.order_id
            WHEN MATCHED AND f.row_hash <> s.row_hash THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""")
        con.execute("COMMIT")
        total = con.execute("SELECT count(*), round(sum(net_revenue), 2) FROM etl_fact_orders").fetchone()
        dims = {n: con.execute(f"SELECT count(*) FROM etl_{n}").fetchone()[0]
                for n in ("dim_customer", "dim_product", "dim_store")}
        con.close()
        print(f"[Load] etl_fact_orders: inserted={inserted} updated={updated} "
              f"unchanged={t['fact_rows'] - inserted - updated} | table rows={total[0]} net_revenue={total[1]}")
        print(f"[Load] dimensions: {dims}")
        return {"inserted": int(inserted), "updated": int(updated), "fact_rows": int(total[0])}

    # ── RECONCILE ────────────────────────────────────────────────────────────
    @task
    def reconcile_payments(t, loaded):
        import pandas as pd
        from nammamart_sikandar.reconcile import reconcile

        _, _, wh, run_id, _ = _ctx()
        con = C.connect(wh)
        orders = con.execute(
            "SELECT order_id, order_status, order_amount, payment_mode FROM etl_fact_orders").fetchdf()
        pays = pd.read_pickle(t["payments_path"])
        export_ids = set(pd.read_pickle(t["orders_path"]).order_id)
        rec = reconcile(orders, pays, export_ids)
        rec["run_id"] = run_id
        rec["detected_at"] = datetime.now()
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM rec")
        summary = con.execute("""SELECT mismatch_reason, count(*) n, round(sum(abs(difference)), 2) amount
                                 FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC""").fetchdf()
        con.close()
        print(f"[Reconcile] {len(rec)} mismatches\n{summary.to_string(index=False)}")
        return len(rec)

    # ── REPORT ───────────────────────────────────────────────────────────────
    @task
    def build_reports(_rec):
        _, _, wh, _, _ = _ctx()
        con = C.connect(wh)
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT f.order_date, s.zone, count(*) AS delivered_orders,
                   round(sum(f.net_revenue), 2) AS net_revenue
            FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY ALL ORDER BY 1, 2""")
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category, count(*) AS delivered_orders, sum(f.quantity) AS units,
                   round(sum(f.net_revenue), 2) AS net_revenue, round(sum(f.cost_amount), 2) AS cost,
                   round(sum(f.gross_margin), 2) AS gross_margin,
                   round(100 * sum(f.gross_margin) / nullif(sum(f.net_revenue), 0), 2) AS margin_pct
            FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY ALL ORDER BY net_revenue DESC""")
        zone = con.execute("""SELECT zone, sum(delivered_orders) orders, round(sum(net_revenue),2) net_revenue
                              FROM etl_daily_zone_revenue GROUP BY 1 ORDER BY 1""").fetchdf()
        cat = con.execute("SELECT * FROM etl_category_margin").fetchdf()
        days = con.execute("SELECT count(DISTINCT order_date) FROM etl_daily_zone_revenue").fetchone()[0]
        con.close()
        print(f"[Report] etl_daily_zone_revenue ({days} days) - totals by zone:\n{zone.to_string(index=False)}")
        print(f"[Report] etl_category_margin:\n{cat.to_string(index=False)}")
        return "ok"

    # ── RUN AUDIT (always runs) ──────────────────────────────────────────────
    @task(trigger_rule="all_done", retries=0)
    def run_audit():
        ctx, _, wh, run_id, _ = _ctx()
        ti = ctx["ti"]

        def pull(task_id):
            try:
                return ti.xcom_pull(task_ids=task_id)
            except Exception:
                return None

        extracted = sum((pull(f"extract_{f}") or {}).get("rows", 0) for f in C.SCHEMAS)
        loaded = pull("load_star_schema") or {}
        quarantined = pull("quarantine_failed_rows") or 0
        finished = pull("build_reports") == "ok"
        dr = ctx["dag_run"]
        start = getattr(dr, "start_date", None) or ctx.get("logical_date")
        row = {
            "run_id": run_id, "dag_id": DAG_ID,
            "start_time": pendulum.instance(start).in_tz("UTC").naive() if start else None,
            "end_time": pendulum.now("UTC").naive(),
            "rows_extracted": int(extracted),
            "rows_loaded": int(loaded.get("inserted", 0) + loaded.get("updated", 0)),
            "rows_quarantined": int(quarantined),
            "status": "SUCCESS" if finished else "FAILED",
        }
        con = C.connect(wh)
        C.write_pipeline_run(con, row)
        con.close()
        print(f"[Audit] {row}")
        if not finished:
            raise AirflowFailException("[Audit] run did not complete - see failed tasks above")

    # ── WIRING ───────────────────────────────────────────────────────────────
    eo, ep, ec, epr, es = extract_orders(), extract_payments(), extract_customers(), extract_products(), extract_stores()
    vc, vp, vs = validate_customers(ec), validate_products(epr), validate_stores(es)
    vo = validate_orders(eo, vc, vp, vs)
    vpay = validate_payments(ep, vo)
    dq = record_dq_results(vo, vpay, vc, vp, vs)
    q = quarantine_failed_rows(vo, vpay, vc, vp, vs)
    t = transform(vo, vpay, vc, vp, vs)
    q >> t
    loaded = load_star_schema(t)
    rec = reconcile_payments(t, loaded)
    report = build_reports(rec)
    [dq, report] >> run_audit()


nammamart_etl_sikandar()
