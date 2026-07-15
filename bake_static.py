"""Bake static JSON data files for the GitHub Pages build (docs/data/).

The static site can't run DuckDB over the 2.9GB parquet, so it works from
pre-aggregated cubes:
  - cube_month_code.json : [monthIdx, codeIdx, paid, patients, claim_lines, n]
  - cube_code_bucket.json: [codeIdx, bucket, paid, n]
  - top_providers.json   : per-code and overall top-12 billing/servicing NPIs
  - top_rows.json        : the 2,000 largest rows by paid (table sample)
  - meta.json            : months, code list, dataset totals
  - dash_default.json / dash_outliers.json : the two pre-baked full dashboards
"""
import duckdb, json, os, shutil

HERE = os.path.dirname(os.path.abspath(__file__))
P = os.path.join(HERE, "data", "spending.parquet")
NPI_LOOKUP = os.path.join(HERE, "data", "npi_state.parquet")
OUT = os.path.join(HERE, "docs", "data")
os.makedirs(OUT, exist_ok=True)

con = duckdb.connect()
con.execute(f"SET threads TO {os.cpu_count() or 8}; SET memory_limit='8GB';")

codes = [r[0] for r in con.execute(
    f"SELECT DISTINCT hcpcs FROM '{P}' WHERE hcpcs IS NOT NULL ORDER BY hcpcs").fetchall()]
months = [r[0] for r in con.execute(
    f"SELECT DISTINCT month FROM '{P}' WHERE month IS NOT NULL ORDER BY month").fetchall()]
states = [r[0] for r in con.execute(
    f"SELECT DISTINCT billing_state FROM '{P}' WHERE billing_state IS NOT NULL "
    "AND len(billing_state)=2 ORDER BY 1").fetchall()]
code_idx = {c: i for i, c in enumerate(codes)}
month_idx = {m: i for i, m in enumerate(months)}
state_idx = {s: i for i, s in enumerate(states)}

print("cube month x code ...")
rows = con.execute(f"""
    SELECT month, hcpcs, round(sum(paid),2), sum(patients), sum(claim_lines), count(*)
    FROM '{P}' WHERE month IS NOT NULL AND hcpcs IS NOT NULL
    GROUP BY month, hcpcs
""").fetchall()
cube = [[month_idx[m], code_idx[c], p, pt, cl, n] for m, c, p, pt, cl, n in rows]
with open(os.path.join(OUT, "cube_month_code.json"), "w") as f:
    json.dump(cube, f, separators=(",", ":"))
print("  rows:", len(cube))

print("cube code x bucket ...")
rows = con.execute(f"""
    SELECT hcpcs, CAST(floor(log10(paid)) AS INT), round(sum(paid),2), count(*)
    FROM '{P}' WHERE hcpcs IS NOT NULL AND paid > 0
    GROUP BY 1, 2
""").fetchall()
with open(os.path.join(OUT, "cube_code_bucket.json"), "w") as f:
    json.dump([[code_idx[c], b, p, n] for c, b, p, n in rows], f, separators=(",", ":"))

print("cube month x state ...")
rows = con.execute(f"""
    SELECT month, billing_state, round(sum(paid),2), sum(patients), sum(claim_lines), count(*)
    FROM '{P}' WHERE month IS NOT NULL AND billing_state IS NOT NULL AND len(billing_state)=2
    GROUP BY 1, 2
""").fetchall()
with open(os.path.join(OUT, "cube_month_state.json"), "w") as f:
    json.dump([[month_idx[m], state_idx[s], p, pt, cl, n] for m, s, p, pt, cl, n in rows],
              f, separators=(",", ":"))
print("  rows:", len(rows))

print("cube state x code ...")
rows = con.execute(f"""
    SELECT billing_state, hcpcs, round(sum(paid),2), sum(patients), sum(claim_lines), count(*)
    FROM '{P}' WHERE hcpcs IS NOT NULL AND billing_state IS NOT NULL AND len(billing_state)=2
    GROUP BY 1, 2
""").fetchall()
with open(os.path.join(OUT, "cube_state_code.json"), "w") as f:
    json.dump([[state_idx[s], code_idx[c], p, pt, cl, n] for s, c, p, pt, cl, n in rows],
              f, separators=(",", ":"))
print("  rows:", len(rows))

print("top providers per state ...")
tops_state = {}
for col, key in (("billing_npi", "billing"), ("servicing_npi", "servicing")):
    rows = con.execute(f"""
        WITH g AS (
            SELECT billing_state AS st, {col} AS npi, round(sum(paid),2) AS paid,
                   sum(patients) AS patients, sum(claim_lines) AS claim_lines,
                   row_number() OVER (PARTITION BY billing_state ORDER BY sum(paid) DESC) AS rk
            FROM '{P}'
            WHERE billing_state IS NOT NULL AND len(billing_state)=2 AND {col} IS NOT NULL
            GROUP BY billing_state, {col}
        )
        SELECT g.st, g.npi, g.paid, g.patients, g.claim_lines, l.name
        FROM g LEFT JOIN '{NPI_LOOKUP}' l ON g.npi = l.npi WHERE g.rk <= 12
    """).fetchall()
    per_state = {}
    for s, npi, p, pt, cl, nm in rows:
        per_state.setdefault(state_idx[s], []).append([npi, p, pt, cl, nm])
    tops_state[key] = per_state
with open(os.path.join(OUT, "top_providers_state.json"), "w") as f:
    json.dump(tops_state, f, separators=(",", ":"))

print("top providers per code ...")
tops = {}
for col, key in (("billing_npi", "billing"), ("servicing_npi", "servicing")):
    rows = con.execute(f"""
        WITH g AS (
            SELECT hcpcs, {col} AS npi, round(sum(paid),2) AS paid,
                   sum(patients) AS patients, sum(claim_lines) AS claim_lines,
                   row_number() OVER (PARTITION BY hcpcs ORDER BY sum(paid) DESC) AS rk
            FROM '{P}' WHERE hcpcs IS NOT NULL AND {col} IS NOT NULL
            GROUP BY hcpcs, {col}
        )
        SELECT g.hcpcs, g.npi, g.paid, g.patients, g.claim_lines, l.name
        FROM g LEFT JOIN '{NPI_LOOKUP}' l ON g.npi = l.npi WHERE g.rk <= 12
    """).fetchall()
    per_code = {}
    for c, npi, p, pt, cl, nm in rows:
        per_code.setdefault(code_idx[c], []).append([npi, p, pt, cl, nm])
    tops[key] = per_code
    print(f"  {key}: {len(rows)} entries")
with open(os.path.join(OUT, "top_providers.json"), "w") as f:
    json.dump(tops, f, separators=(",", ":"))

print("top rows sample ...")
rows = con.execute(f"""
    SELECT billing_npi, billing_state, servicing_npi, hcpcs, month, patients, claim_lines, paid
    FROM '{P}' ORDER BY paid DESC LIMIT 2000
""").fetchall()
with open(os.path.join(OUT, "top_rows.json"), "w") as f:
    json.dump([list(r) for r in rows], f, separators=(",", ":"))

with open(os.path.join(HERE, "data", "stats.json")) as f:
    stats = json.load(f)
stats["codes"] = codes
stats["states"] = states
with open(os.path.join(OUT, "meta.json"), "w") as f:
    json.dump(stats, f, separators=(",", ":"))

shutil.copy(os.path.join(HERE, "data", "default_dashboard.json"),
            os.path.join(OUT, "dash_default.json"))
# the persisted exclude-outliers cache entry (paid_max=100000000)
for name in os.listdir(os.path.join(HERE, "data", "cache")):
    with open(os.path.join(HERE, "data", "cache", name)) as f:
        entry = json.load(f)
    if "paid_max" in entry["key"]:
        with open(os.path.join(OUT, "dash_outliers.json"), "w") as f:
            json.dump(entry["result"], f, separators=(",", ":"))
        break

for name in sorted(os.listdir(OUT)):
    print(f"{name}: {os.path.getsize(os.path.join(OUT, name))/1e6:.1f} MB")

print("sector leaders ...")
with open(os.path.join(HERE, "data", "sectors.json")) as f:
    sector_map = json.load(f)
con.execute("CREATE TEMP TABLE sector_map(hcpcs VARCHAR, sector VARCHAR)")
con.executemany("INSERT INTO sector_map VALUES (?, ?)", list(sector_map.items()))
rows = con.execute(f"""
    WITH g AS (
      SELECT m.sector, p.billing_npi AS npi, round(sum(p.paid),2) AS paid,
             sum(p.patients) AS patients, sum(p.claim_lines) AS claim_lines,
             row_number() OVER (PARTITION BY m.sector ORDER BY sum(p.paid) DESC) AS rk
      FROM '{P}' p JOIN sector_map m ON p.hcpcs = m.hcpcs
      WHERE p.billing_npi IS NOT NULL
      GROUP BY 1, 2
    )
    SELECT g.sector, g.npi, g.paid, g.patients, g.claim_lines, l.name
    FROM g LEFT JOIN '{NPI_LOOKUP}' l ON g.npi = l.npi
    WHERE g.rk <= 8 ORDER BY g.sector, g.paid DESC
""").fetchall()
leaders = {}
for sect, npi, p, pt, cl, nm in rows:
    leaders.setdefault(sect, []).append([npi, p, pt, cl, nm])
with open(os.path.join(OUT, "sector_leaders.json"), "w") as f:
    json.dump(leaders, f, separators=(",", ":"))
print("  sectors:", len(leaders))

print("provider search index ...")
rows = con.execute(f"""
    WITH t AS (
      SELECT billing_npi AS npi, hcpcs, sum(paid) AS p
      FROM '{P}' WHERE billing_npi IS NOT NULL AND hcpcs IS NOT NULL
      GROUP BY 1, 2
    ), tot AS (
      SELECT npi, round(sum(p),2) AS total FROM t GROUP BY 1
      HAVING sum(p) >= 1000000
    ), ranked AS (
      SELECT t.npi, t.hcpcs, t.p,
             row_number() OVER (PARTITION BY t.npi ORDER BY t.p DESC) AS rk
      FROM t JOIN tot ON t.npi = tot.npi
    )
    SELECT r.npi, any_value(tot.total),
           list(r.hcpcs ORDER BY r.p DESC),
           round(sum(r.p),2) AS paid_on_codes,
           any_value(l.name), any_value(l.state)
    FROM ranked r
    JOIN tot ON r.npi = tot.npi
    LEFT JOIN '{NPI_LOOKUP}' l ON r.npi = l.npi
    WHERE r.rk <= 8
    GROUP BY r.npi
""").fetchall()
index = [[npi, nm, st, total, [code_idx[c] for c in cl if c in code_idx], poc]
         for npi, total, cl, poc, nm, st in rows]
index.sort(key=lambda e: -e[3])
with open(os.path.join(OUT, "provider_index.json"), "w") as f:
    json.dump(index, f, separators=(",", ":"))
print(f"  providers: {len(index)}, size: {os.path.getsize(os.path.join(OUT,'provider_index.json'))/1e6:.1f} MB")
