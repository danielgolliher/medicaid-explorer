# Medicaid Provider Spending Explorer

Web app for exploring `medicaid-provider-spending.csv` (238M rows, 11 GB,
2018–2024).

**Hosted version:** https://danielgolliher.github.io/medicaid-explorer/ — a
static snapshot served from `docs/` via GitHub Pages, behind a client-side
password gate. GitHub Pages can't run the DuckDB backend, so the static build
works from pre-aggregated JSON (`bake_static.py`): month×code and code×bucket
cubes, per-code top-12 providers, and the 2,000 largest rows. That supports
date-range and HCPCS filtering with adaptive charts; NPI search and full
row-level browsing need the local app below. Note the password gate is a
deterrent, not security — the page and its data are public to anyone who reads
the JavaScript, and the repo itself is public. The CSV is converted once to a 2.9 GB Parquet snapshot; a small
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
.venv/bin/python add_state.py          # join billing NPI -> NPPES state, rebake defaults
.venv/bin/python bake_static.py        # regenerate docs/data for the Pages build
```

`add_state.py` expects the NPPES bulk file at `data/nppes.zip`
(https://download.cms.gov/nppes/NPI_Files.html); it keeps the extracted
NPI→state mapping in `data/npi_state.parquet` for reuse.

## State dimension

There is no state column in the source data. `billing_state` is the billing
provider's NPPES practice-location registration state (96% row coverage) — an
approximation, since national providers bill many states' programs from one
registration. The UI labels it accordingly. Rows with blank billing NPIs
(including the extreme-outlier rows) have no state.

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

## Suggestions board

The hosted page has a "Suggestions" section (same mechanism as the MI Mythos
briefs' comments): shared comments stored by the `mi-mythos-comments`
Cloudflare Worker (KV-backed, page key `/medicaid-explorer`, IP-derived
"Commenter N" identities with optional display names, author-only deletes).
The Worker source lives in the mi-mythos repo under `comments-backend/`. If
the Worker is unreachable the board degrades to device-local localStorage.

## Data caveats

The raw file contains implausible outliers (single rows in the trillions of
dollars, concentrated in HCPCS code `20` in 2018–2019, and blank billing
NPIs). The "Exclude extreme rows" preset (paid ≤ $100M per row) screens them
out; nothing is dropped from the underlying data.
