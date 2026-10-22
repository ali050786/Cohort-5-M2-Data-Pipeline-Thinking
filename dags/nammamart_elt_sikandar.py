"""
dags/nammamart_elt_sikandar.py
------------------------------
NammaMart Quick Commerce: ELT pipeline

Extract -> Load raw (exact copy, all text) -> Transform in DuckDB with SQL (staging -> mart)

  load_raw_orders ──► load_raw_orders_increment ─┐
  load_raw_payments ─────────────────────────────┤
  load_raw_customers ─► stg_customers ──┐        │
  load_raw_products ──► stg_products ───┼────────┴─► stg_orders_merge ─► stg_orders_quality ─► stg_payments
  load_raw_stores ────► stg_stores ─────┘                                                        │
                                                    ┌────────────────────────────────────────────┤
                                       mart_* (6 tables, SQL) ─────────────► record_dq_results ──┤
                                                                                      run_audit (all_done)

Layers in include/warehouse_sikandar.duckdb
  raw_*      every row exactly as received + _loaded_at, _source_file, _run_id (never cleaned/deleted)
  stg_*      typed, standardised, de-duplicated, PII masked, dq_status + failure_reason
  mart_*     business tables answering the six client questions
All transformations are .sql files in include/sql/sikandar/.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

from airflow.exceptions import AirflowFailException  # noqa: E402
from airflow.sdk import Param, dag, get_current_context, task  # noqa: E402

from nammamart_sikandar import common as C  # noqa: E402

DAG_ID = "nammamart_elt_sikandar"

default_args = {
    "owner": "sikandar",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": C.slack_on_failure,
}

MARTS = ["mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
         "mart_payment_reconciliation", "mart_top_customers", "mart_dq_rejections"]


def _ctx():
    ctx = get_current_context()
    params = ctx["params"]
    data_dir = C.resolve(params.get("data_dir"), C.DEFAULT_DATA_DIR)
    wh = C.resolve(params.get("warehouse"), C.DEFAULT_WAREHOUSE)
    return ctx, data_dir, wh, ctx["run_id"]


def _load(file_key: str):
    from nammamart_sikandar.elt import load_raw
    _, data_dir, wh, run_id = _ctx()
    con = C.connect(wh)
    try:
        return load_raw(con, data_dir / f"{file_key}.csv", file_key, run_id)
    finally:
        con.close()


def _sql(*files: str):
    from nammamart_sikandar.elt import run_sql
    _, _, wh, run_id = _ctx()
    con = C.connect(wh)
    try:
        for f in files:
            run_sql(con, f, run_id=run_id, dag_id=DAG_ID)
    finally:
        con.close()


def _print(con, sql, title):
    df = con.execute(sql).fetchdf()
    print(f"{title}\n{df.to_string(index=False)}")


@dag(
    dag_id=DAG_ID,
    description="NammaMart ELT: land every raw row in DuckDB, then clean and model with SQL",
    schedule="0 9 * * 1-6",                                      # 09:00 IST Mon-Sat
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={
        "data_dir": Param("data/nammamart", type="string",
                          description="Source folder, relative to include/ (use a copy for experiments)"),
        "warehouse": Param("warehouse_sikandar.duckdb", type="string",
                           description="DuckDB file, relative to include/"),
    },
    tags=["nammamart", "elt", "sikandar", "module2"],
)
def nammamart_elt_sikandar():

    # ── EXTRACT + LOAD RAW (parallel) ────────────────────────────────────────
    @task
    def load_raw_orders():
        return _load("orders")

    @task
    def load_raw_payments():
        return _load("payments")

    @task
    def load_raw_customers():
        return _load("customers")

    @task
    def load_raw_products():
        return _load("products")

    @task
    def load_raw_stores():
        return _load("stores")

    @task
    def load_raw_orders_increment(_base):
        """Append any orders_<date>_increment.csv (oldest first) to raw_orders."""
        from nammamart_sikandar.elt import load_raw
        _, data_dir, wh, run_id = _ctx()
        files = sorted(data_dir.glob(C.INCREMENT_GLOB))
        if not files:
            print("[Load raw] no increment files found")
        con = C.connect(wh)
        try:
            return [load_raw(con, f, "orders", run_id) for f in files]
        finally:
            con.close()

    # ── STAGING (SQL) ────────────────────────────────────────────────────────
    @task
    def stg_customers(_raw):
        _sql("staging/stg_customers.sql")

    @task
    def stg_products(_raw):
        _sql("staging/stg_products.sql")

    @task
    def stg_stores(_raw):
        _sql("staging/stg_stores.sql")

    @task
    def stg_orders_merge(base, increments):
        """MERGE (upsert on order_id) every orders file newly loaded in this run, base file first."""
        from nammamart_sikandar.elt import run_sql
        _, _, wh, run_id = _ctx()
        con = C.connect(wh)
        stats = []
        try:
            run_sql(con, "staging/stg_orders_create.sql")
            for load in [base] + list(increments or []):
                if load["skipped"]:
                    print(f"[MERGE] {load['file']}: already merged earlier, skipping")
                    continue
                before = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
                hist_before = con.execute("SELECT count(*) FROM stg_orders_status_history").fetchone()[0]
                src_rows = con.execute(
                    "SELECT count(DISTINCT order_id) FROM raw_orders WHERE _load_id = ?", [load["load_id"]]
                ).fetchone()[0]
                run_sql(con, "staging/stg_orders_merge.sql", load_id=load["load_id"], run_id=run_id)
                after = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
                changed = con.execute("SELECT count(*) FROM stg_orders_status_history").fetchone()[0] - hist_before
                written = con.execute("SELECT count(*) FROM stg_orders WHERE _load_id = ?",
                                      [load["load_id"]]).fetchone()[0]
                s = {"file": load["file"], "source_orders": int(src_rows), "inserted": int(after - before),
                     "updated": int(written - (after - before)),
                     "kept_newer_version": int(src_rows - written), "status_changes": int(changed)}
                stats.append(s)
                print(f"[MERGE] {s}")
            total = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
            print(f"[MERGE] stg_orders now holds {total} orders")
        finally:
            con.close()
        return stats

    @task
    def stg_orders_quality(_merged, _c, _p, _s):
        _sql("staging/stg_orders_quality.sql")
        _, _, wh, _ = _ctx()
        con = C.connect(wh)
        _print(con, "SELECT dq_status, count(*) n FROM stg_orders GROUP BY 1 ORDER BY 1", "[DQ] stg_orders")
        con.close()

    @task
    def stg_payments(_orders):
        _sql("staging/stg_payments.sql")

    # ── MARTS (SQL) ──────────────────────────────────────────────────────────
    @task
    def build_mart_view(_pay):
        _sql("mart/00_mart_orders_view.sql")

    def make_mart(name):
        @task(task_id=name)
        def _mart(_view):
            _sql(f"mart/{name}.sql")
            _, _, wh, _ = _ctx()
            con = C.connect(wh)
            _print(con, f"SELECT * FROM {name} LIMIT 12", f"[Mart] {name} (first rows)")
            con.close()
            return name
        return _mart

    # ── DQ SCORECARD + REPORT ────────────────────────────────────────────────
    @task
    def record_dq_results(*_marts):
        from nammamart_sikandar.elt import run_sql
        _, _, wh, run_id = _ctx()
        con = C.connect(wh)
        C.ensure_audit_tables(con)
        run_sql(con, "dq/dq_results.sql", run_id=run_id, dag_id=DAG_ID)
        _print(con, f"""SELECT check_name, severity, rows_checked, rows_failed, status FROM dq_results
                        WHERE run_id = '{run_id}' AND dag_id = '{DAG_ID}' AND status <> 'PASS'
                        ORDER BY status, check_name""", "[DQ] non-passing checks this run")
        _print(con, """SELECT zone, sum(delivered_orders) orders, round(sum(net_revenue), 2) net_revenue
                       FROM mart_daily_store_kpis GROUP BY 1 ORDER BY 1""", "[Report] net revenue by zone")
        con.close()
        return "ok"

    @task(trigger_rule="all_done", retries=0)
    def run_audit():
        ctx, _, wh, run_id = _ctx()
        ti = ctx["ti"]

        def pull(task_id):
            try:
                return ti.xcom_pull(task_ids=task_id)
            except Exception:
                return None

        extracted = sum((pull(f"load_raw_{f}") or {}).get("rows", 0) for f in C.SCHEMAS)
        extracted += sum(x.get("rows", 0) for x in (pull("load_raw_orders_increment") or []))
        merged = pull("stg_orders_merge") or []
        finished = pull("record_dq_results") == "ok"
        quarantined = 0
        con = C.connect(wh)
        try:
            quarantined = con.execute("""
                SELECT (SELECT count(*) FROM stg_orders WHERE dq_status = 'FAIL')
                     + (SELECT coalesce(sum(dup_copies), 0) FROM stg_orders)
                     + (SELECT count(*) FROM stg_payments WHERE dq_status = 'FAIL')
                     + (SELECT count(*) FROM stg_customers WHERE dq_status = 'FAIL')
                     + (SELECT count(*) FROM stg_products WHERE dq_status = 'FAIL')
                     + (SELECT count(*) FROM stg_stores WHERE dq_status = 'FAIL')""").fetchone()[0]
        except Exception:
            pass
        dr = ctx["dag_run"]
        start = getattr(dr, "start_date", None) or ctx.get("logical_date")
        row = {
            "run_id": run_id, "dag_id": DAG_ID,
            "start_time": pendulum.instance(start).in_tz("UTC").naive() if start else None,
            "end_time": pendulum.now("UTC").naive(),
            "rows_extracted": int(extracted),
            "rows_loaded": int(sum(m["inserted"] + m["updated"] for m in merged)),
            "rows_quarantined": int(quarantined),
            "status": "SUCCESS" if finished else "FAILED",
        }
        C.write_pipeline_run(con, row)
        con.close()
        print(f"[Audit] {row}")
        if not finished:
            raise AirflowFailException("[Audit] run did not complete - see failed tasks above")

    # ── WIRING ───────────────────────────────────────────────────────────────
    ro, rp, rc, rpr, rs = (load_raw_orders(), load_raw_payments(), load_raw_customers(),
                           load_raw_products(), load_raw_stores())
    rinc = load_raw_orders_increment(ro)
    sc, sp, ss = stg_customers(rc), stg_products(rpr), stg_stores(rs)
    merged = stg_orders_merge(ro, rinc)
    q = stg_orders_quality(merged, sc, sp, ss)
    pay = stg_payments(q)
    rp >> pay
    view = build_mart_view(pay)
    marts = [make_mart(m)(view) for m in MARTS]
    dq = record_dq_results(*marts)
    dq >> run_audit()


nammamart_elt_sikandar()
