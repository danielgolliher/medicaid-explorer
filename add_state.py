"""Add billing_state to the spending Parquet by joining NPPES registration state.

Pipeline (one-time, ~10 min):
  1. Extract NPI -> practice-location state from the NPPES bulk file
     (data/nppes.zip, downloaded from download.cms.gov/nppes).
  2. Rebuild data/spending.parquet with a billing_state column.
  3. Refresh stats.json (adds `states`), the precomputed default dashboard,
     and the exclude-outliers cache entry (dashboard shape gained top_states).
"""
import duckdb, glob, hashlib, json, os, subprocess, time

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
P = os.path.join(DATA, "spending.parquet")
NPI_STATE = os.path.join(DATA, "npi_state.parquet")

con = duckdb.connect()
con.execute(f"SET threads TO {os.cpu_count() or 8}; SET memory_limit='8GB';")

if not os.path.exists(NPI_STATE):
    print("extracting NPPES csv ...")
    subprocess.run(["unzip", "-o", "-d", DATA, os.path.join(DATA, "nppes.zip"),
                    "npidata_pfile_*-*.csv"], check=True,
                   stdout=subprocess.DEVNULL)
    csv = [f for f in glob.glob(os.path.join(DATA, "npidata_pfile_*.csv"))
           if "fileheader" not in f][0]
    print("building npi -> state parquet ...")
    t0 = time.time()
    con.execute(f"""
        COPY (
            SELECT CAST(NPI AS VARCHAR) AS npi,
                   upper(trim("Provider Business Practice Location Address State Name")) AS state
            FROM read_csv('{csv}', header=true, all_varchar=true)
            WHERE NPI IS NOT NULL
        ) TO '{NPI_STATE}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    print(f"  done in {time.time()-t0:.0f}s")
    os.remove(csv)

print("rebuilding spending.parquet with billing_state ...")
t0 = time.time()
tmp = P + ".tmp"
con.execute(f"""
    COPY (
        SELECT s.*, n.state AS billing_state
        FROM '{P}' s LEFT JOIN '{NPI_STATE}' n ON s.billing_npi = n.npi
        ORDER BY s.month, s.hcpcs
    ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
""")
os.replace(tmp, P)
print(f"  done in {time.time()-t0:.0f}s, size={os.path.getsize(P)/1e9:.2f}GB")

states = [r[0] for r in con.execute(
    f"SELECT DISTINCT billing_state FROM '{P}' WHERE billing_state IS NOT NULL "
    "AND len(billing_state)=2 ORDER BY 1").fetchall()]
with open(os.path.join(DATA, "stats.json")) as f:
    stats = json.load(f)
stats["states"] = states
with open(os.path.join(DATA, "stats.json"), "w") as f:
    json.dump(stats, f)
print(f"{len(states)} states; coverage:",
      con.execute(f"SELECT round(100.0*count(billing_state)/count(*),1) FROM '{P}'").fetchone()[0], "%")

con.close()
import server  # noqa: E402  (fresh connection over the new parquet)
print("rebaking default dashboard ...")
d = server._dashboard({})
with open(os.path.join(DATA, "default_dashboard.json"), "w") as f:
    json.dump(d, f)
print("rebaking exclude-outliers cache ...")
key = json.dumps({"paid_max": ["100000000"]})
result = server._dashboard({"paid_max": ["100000000"]})
for old in glob.glob(os.path.join(DATA, "cache", "*.json")):
    os.remove(old)
with open(os.path.join(DATA, "cache",
          hashlib.md5(key.encode()).hexdigest() + ".json"), "w") as f:
    json.dump({"key": key, "result": result}, f)
print("all done")
