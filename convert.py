"""One-time conversion: 11GB CSV -> Parquet + a small stats JSON for the app."""
import duckdb, json, os, time

SRC = "/Users/danielgolliher/Downloads/medicaid-provider-spending.csv"
OUT = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(OUT, exist_ok=True)
PARQUET = os.path.join(OUT, "spending.parquet")

con = duckdb.connect()
con.execute("SET threads TO 8; SET memory_limit='8GB';")

t0 = time.time()
con.execute(f"""
    COPY (
        SELECT
            CAST(BILLING_PROVIDER_NPI_NUM AS VARCHAR)    AS billing_npi,
            CAST(SERVICING_PROVIDER_NPI_NUM AS VARCHAR)  AS servicing_npi,
            CAST(HCPCS_CODE AS VARCHAR)                  AS hcpcs,
            CLAIM_FROM_MONTH                             AS month,
            CAST(TOTAL_PATIENTS AS BIGINT)               AS patients,
            CAST(TOTAL_CLAIM_LINES AS BIGINT)            AS claim_lines,
            CAST(TOTAL_PAID AS DOUBLE)                   AS paid
        FROM read_csv('{SRC}', header=true, all_varchar=false,
                       types={{'BILLING_PROVIDER_NPI_NUM':'VARCHAR','SERVICING_PROVIDER_NPI_NUM':'VARCHAR','HCPCS_CODE':'VARCHAR'}})
        ORDER BY month, hcpcs
    ) TO '{PARQUET}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 1000000)
""")
print(f"parquet written in {time.time()-t0:.0f}s, size={os.path.getsize(PARQUET)/1e9:.2f}GB")

# Precompute global stats the UI needs at load time
stats = {}
stats["rows"] = con.execute(f"SELECT count(*) FROM '{PARQUET}'").fetchone()[0]
stats["months"] = [r[0] for r in con.execute(
    f"SELECT DISTINCT month FROM '{PARQUET}' WHERE month IS NOT NULL ORDER BY month").fetchall()]
stats["hcpcs_count"] = con.execute(f"SELECT count(DISTINCT hcpcs) FROM '{PARQUET}'").fetchone()[0]
stats["billing_npis"] = con.execute(f"SELECT count(DISTINCT billing_npi) FROM '{PARQUET}'").fetchone()[0]
stats["servicing_npis"] = con.execute(f"SELECT count(DISTINCT servicing_npi) FROM '{PARQUET}'").fetchone()[0]
stats["total_paid"] = con.execute(f"SELECT sum(paid) FROM '{PARQUET}'").fetchone()[0]
with open(os.path.join(OUT, "stats.json"), "w") as f:
    json.dump(stats, f)
print("stats:", stats)
