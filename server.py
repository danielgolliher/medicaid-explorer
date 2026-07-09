"""Medicaid Provider Spending Explorer — local query server.

Serves the frontend and answers aggregate/row queries against the Parquet
snapshot with DuckDB. One scan per dashboard refresh via GROUPING SETS.
"""
import duckdb
import hashlib
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

HERE = os.path.dirname(os.path.abspath(__file__))
PARQUET = os.path.join(HERE, "data", "spending.parquet")
STATS = os.path.join(HERE, "data", "stats.json")
PORT = 8734

con = duckdb.connect()
con.execute(f"SET threads TO {os.cpu_count() or 8}; SET memory_limit='6GB';")
con.execute("SET enable_progress_bar=false;")
con_lock = threading.Lock()

TOP_N = 12

# Dashboard results are cached per filter combination. Results that took a
# full scan (>3s) also persist to disk so they stay warm across restarts;
# the unfiltered landing-page result is precomputed the same way.
DEFAULT_DASH = os.path.join(HERE, "data", "default_dashboard.json")
CACHE_DIR = os.path.join(HERE, "data", "cache")
os.makedirs(CACHE_DIR, exist_ok=True)
dash_cache = {}
dash_cache_lock = threading.Lock()


def build_where(q):
    """Translate query params into a WHERE clause + bind params."""
    clauses, params = [], []
    if q.get("month_from"):
        clauses.append("month >= ?")
        params.append(q["month_from"][0])
    if q.get("month_to"):
        clauses.append("month <= ?")
        params.append(q["month_to"][0])
    if q.get("hcpcs"):
        codes = [c.strip().upper() for c in q["hcpcs"][0].split(",") if c.strip()]
        if codes:
            clauses.append("upper(hcpcs) IN (%s)" % ",".join("?" * len(codes)))
            params.extend(codes)
    for field, col in (("billing_npi", "billing_npi"), ("servicing_npi", "servicing_npi"),
                       ("state", "billing_state")):
        if q.get(field):
            val = q[field][0].strip()
            if val:
                clauses.append(f"{col} = ?")
                params.append(val.upper() if field == "state" else val)
    if q.get("paid_min"):
        clauses.append("paid >= ?")
        params.append(float(q["paid_min"][0]))
    if q.get("paid_max"):
        clauses.append("paid <= ?")
        params.append(float(q["paid_max"][0]))
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def dashboard(q):
    key = json.dumps({k: q[k] for k in sorted(q)
                      if k in ("month_from", "month_to", "hcpcs", "billing_npi",
                               "servicing_npi", "state", "paid_min", "paid_max")})
    with dash_cache_lock:
        if key in dash_cache:
            return dash_cache[key]
    t0 = time.time()
    result = _dashboard(q)
    if time.time() - t0 > 3:
        path = os.path.join(CACHE_DIR, hashlib.md5(key.encode()).hexdigest() + ".json")
        with open(path, "w") as f:
            json.dump({"key": key, "result": result}, f)
    with dash_cache_lock:
        if len(dash_cache) > 64:
            dash_cache.clear()
        dash_cache[key] = result
    return result


def _dashboard(q):
    where, params = build_where(q)
    sql = f"""
        SELECT
            month, hcpcs, billing_npi, servicing_npi, billing_state,
            CASE WHEN paid > 0 THEN floor(log10(paid)) END AS bucket,
            GROUPING(month, hcpcs, billing_npi, servicing_npi, billing_state,
                     CASE WHEN paid > 0 THEN floor(log10(paid)) END) AS gid,
            sum(paid) AS paid, sum(patients) AS patients,
            sum(claim_lines) AS claim_lines, count(*) AS n
        FROM '{PARQUET}' {where}
        GROUP BY GROUPING SETS (
            (), (month), (hcpcs), (billing_npi), (servicing_npi), (billing_state),
            (CASE WHEN paid > 0 THEN floor(log10(paid)) END)
        )
    """
    with con_lock:
        rows = con.execute(sql, params).fetchall()

    # GROUPING() bit order follows the column list: month is the high bit,
    # bucket the low bit. All-grouped-out = 0b111111 = 63.
    G_TOTAL, G_MONTH, G_HCPCS, G_BILL, G_SERV, G_STATE, G_BUCKET = 63, 31, 47, 55, 59, 61, 62
    summary = {"rows": 0, "paid": 0, "patients": 0, "claim_lines": 0}
    monthly, hcpcs_g, bill_g, serv_g, state_g, bucket_g = [], [], [], [], [], []
    for month, hcpcs, bill, serv, st, bucket, gid, paid, patients, lines, n in rows:
        rec = {"paid": paid or 0, "patients": patients or 0,
               "claim_lines": lines or 0, "n": n}
        if gid == G_TOTAL:
            summary = {"rows": n, "paid": paid or 0, "patients": patients or 0,
                       "claim_lines": lines or 0}
        elif gid == G_MONTH and month is not None:
            monthly.append({"month": month, **rec})
        elif gid == G_HCPCS:
            hcpcs_g.append({"key": hcpcs, **rec})
        elif gid == G_BILL:
            bill_g.append({"key": bill, **rec})
        elif gid == G_SERV:
            serv_g.append({"key": serv, **rec})
        elif gid == G_STATE:
            state_g.append({"key": st, **rec})
        elif gid == G_BUCKET and bucket is not None:
            bucket_g.append({"bucket": int(bucket), **rec})

    monthly.sort(key=lambda r: r["month"])
    bucket_g.sort(key=lambda r: r["bucket"])
    top = lambda g: sorted(g, key=lambda r: r["paid"], reverse=True)[:TOP_N]
    summary["hcpcs_n"] = sum(1 for r in hcpcs_g if r["key"] is not None)
    summary["billing_n"] = sum(1 for r in bill_g if r["key"] is not None)
    summary["servicing_n"] = sum(1 for r in serv_g if r["key"] is not None)
    return {
        "summary": summary,
        "monthly": monthly,
        "top_hcpcs": top([r for r in hcpcs_g if r["key"] is not None]),
        "top_billing": top([r for r in bill_g if r["key"] is not None]),
        "top_servicing": top([r for r in serv_g if r["key"] is not None]),
        "top_states": top([r for r in state_g if r["key"] is not None]),
        "hist": bucket_g,
    }


SORTABLE = {"month", "hcpcs", "billing_npi", "servicing_npi", "billing_state",
            "patients", "claim_lines", "paid"}


def table_rows(q):
    where, params = build_where(q)
    sort = q.get("sort", ["paid"])[0]
    if sort not in SORTABLE:
        sort = "paid"
    direction = "ASC" if q.get("dir", ["desc"])[0].lower() == "asc" else "DESC"
    limit = min(int(q.get("limit", ["50"])[0]), 500)
    offset = min(int(q.get("offset", ["0"])[0]), 100000)
    sql = f"""
        SELECT billing_npi, billing_state, servicing_npi, hcpcs, month,
               patients, claim_lines, paid
        FROM '{PARQUET}' {where}
        ORDER BY {sort} {direction} NULLS LAST
        LIMIT {limit} OFFSET {offset}
    """
    with con_lock:
        rows = con.execute(sql, params).fetchall()
    return {"rows": [list(r) for r in rows]}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send_json(self, obj, code=200):
        body = json.dumps(obj, allow_nan=False, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path, ctype):
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        url = urlparse(self.path)
        q = parse_qs(url.query)
        try:
            if url.path == "/":
                self.send_file(os.path.join(HERE, "static", "index.html"),
                               "text/html; charset=utf-8")
            elif url.path == "/api/meta":
                with open(STATS) as f:
                    self.send_json(json.load(f))
            elif url.path == "/api/dashboard":
                self.send_json(dashboard(q))
            elif url.path == "/api/rows":
                self.send_json(table_rows(q))
            elif url.path.startswith("/static/"):
                name = os.path.basename(url.path)
                ctype = ("image/svg+xml" if name.endswith(".svg")
                         else "text/css" if name.endswith(".css")
                         else "application/javascript" if name.endswith(".js")
                         else "application/octet-stream")
                self.send_file(os.path.join(HERE, "static", name), ctype)
            else:
                self.send_error(404)
        except BrokenPipeError:
            pass
        except Exception as e:
            self.send_json({"error": str(e)}, 500)


def load_default_cache():
    if os.path.exists(DEFAULT_DASH):
        with open(DEFAULT_DASH) as f:
            dash_cache["{}"] = json.load(f)
    for name in os.listdir(CACHE_DIR):
        if name.endswith(".json"):
            try:
                with open(os.path.join(CACHE_DIR, name)) as f:
                    entry = json.load(f)
                dash_cache[entry["key"]] = entry["result"]
            except (OSError, KeyError, ValueError):
                pass


if __name__ == "__main__":
    load_default_cache()
    print(f"Serving on http://localhost:{PORT}")
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
