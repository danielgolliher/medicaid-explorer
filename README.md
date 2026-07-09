# Medicaid Provider Spending Explorer

Local web app for exploring `medicaid-provider-spending.csv` (238M rows, 11 GB,
2018–2024). The CSV is converted once to a 2.9 GB Parquet snapshot; a small
Python server answers aggregate queries with DuckDB; the frontend is a single
HTML page with filters, auto-generated charts, and a sortable row browser.

## Run

```bash
cd ~/projects/medicaid-explorer
.venv/bin/python server.py
# open http://localhost:8734
```

## Rebuild the data snapshot

Only needed if the source CSV changes:

```bash
.venv/bin/python convert.py            # CSV -> data/spending.parquet + stats.json
.venv/bin/python -c "import json, server; json.dump(server._dashboard({}), open('data/default_dashboard.json','w'))"
rm -f data/cache/*.json                # drop stale preset caches
```

## How it works

- `convert.py` — one-time CSV → Parquet (zstd, sorted by month+HCPCS so
  month/code filters get row-group pruning and run in well under a second).
- `server.py` — stdlib HTTP server + DuckDB. `/api/dashboard` computes the
  summary, monthly trend, top codes/providers, and a log-scale payment
  histogram in a single scan via `GROUP BY GROUPING SETS`. Results are cached
  per filter combo; anything that needed a slow full scan (>3s, e.g. a
  paid-amount-only filter) is persisted to `data/cache/` so it stays warm
  across restarts. `/api/rows` pages through raw rows.
- `static/index.html` — the whole frontend (no build step, no dependencies).
  Charts are hand-rolled SVG. The chart set adapts to the active filters
  (filter to a code → top servicing providers for it; filter to a provider →
  its top codes), marked with an "Auto" caption. Filter state lives in the URL
  hash, so views are shareable. Light/dark follow the OS.
- `static/*.svg` — logo, header background, and empty-state art.

## Data caveats

The raw file contains implausible outliers (single rows in the trillions of
dollars, concentrated in HCPCS code `20` in 2018–2019, and blank billing
NPIs). The "Exclude extreme rows" preset (paid ≤ $100M per row) screens them
out; nothing is dropped from the underlying data.
