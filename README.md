# Story Protocol IP Token Analysis

📊 This project is a research dashboard for **Story Protocol’s IP token.** It tracks token supply, unlocks, holder concentration, exchange flows, and wallet classifications using on-chain data ingested into PostgreSQL.

🚀 **Live Demo**: [storyip.streamlit.app](https://storyip.streamlit.app)

# Features

## Token Supply & Unlocks
- Circulating vs locked supply (absolute + % over time).
- Unlock schedule with cliffs, linear vesting, allocation categories.
- Horizon selector (today, custom date, full schedule).

## Holder Concentration
- Top-10 / Top-50 share, Gini, and HHI inequality metrics.
- Daily timeseries plots from balance snapshots.

## Exchange Flows
- Net inflows/outflows to labeled CEX hot wallets.
- Proximity to unlocks for sell-pressure monitoring.

## Top Holders
- Latest top holders with labels & heuristic flags (accumulator, seller, redistributor, etc.).

## Clusters (from manual probes)
- Cluster A, Bybit, and MEXC wallets (hot + treasury).
- Displays even if not in top-50 by balance.

## Post-Unlock Behavior
Framework for 7-day sell-through analysis after unlocks.
(No real unlocks fall within the 30-day backfill window; next major unlock is Feb 13, 2026.)
---

## 1. Local Setup

### Clone repo
```bash
git clone https://github.com/prabuddh1/ip-token-analysis.git
cd ip-token-analysis
```
### Python virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
### Postgres (Docker)
```bash
docker run --name storyip-db -e POSTGRES_USER=selini -e POSTGRES_PASSWORD=selini \
  -e POSTGRES_DB=storyip -p 5432:5432 -d postgres:16
```
### Environment variables
Create a .env file in project root:
```bash
# Postgres
DATABASE_URL_PG=postgresql://selini:selini@localhost:5432/storyip

# RPC (HTTP required; WSS optional)
RPC_URL=https://<your-evm-rpc-host>        # e.g. Alchemy, Infura, QuickNode, Ankr
WSS_URL=wss://<your-evm-rpc-host>          # optional, not required for this project
```
Load it in your shell:
```bash
set -a; source .env; set +a
```

## 2. ETL (Extract, Transform, Load)

Run the ETL scripts to populate Postgres:
```bash
python etl/blocks_txns.py
python etl/supply.py
python etl/unlocks.py
python etl/balances_latest.py --days 30
python etl/concentration.py --all
python etl/labels_rules.py
python etl/flows.py
```
This will fill:
- blocks, transactions, ip_transfers
- supply_timeseries, unlock_schedule
- balances_latest, top_holders_snapshot
- address_labels, holder_flags, exchange_flows, concentration_timeseries

## 3. Run Dashboard Locally

```bash
streamlit run dashboard/app.py --server.port=8501 --server.headless=true
```
Open **localhost:8501** in your browser.

## 4. Notes

- **Circulating supply** excludes vesting contracts, team/foundation lockups, and non-circulating allocations.
- **Exchange net flows** = net inflow/outflow to tagged CEX addresses.
- **Post-unlock behavior section:** Story’s next major unlock is Feb 13, 2026 (Foundation 5% cliff + vesting start).
- **With current 30-day backfill**, no realized unlocks exist → dashboard shows an informative placeholder.
- **balances_latest** is large (≈1M rows for full history). For demo, only the latest snapshot is pushed to Neon.
