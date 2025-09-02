# Setup
cp .env.example .env
# edit .env with Story RPC + Postgres creds


# Start services
docker compose up -d postgres
pip install -r requirements.txt
python -m etl.db --init # create schema


# Backfill last X days (e.g., 120 days)
python scripts/backfill.py --days 120 --batch-blocks 500


# Start streamer (follow new blocks)
python scripts/run_stream.py --confirmations 12


# Build time series & labels
python -m etl.unlocks --recompute
python -m etl.labels_rules --recompute
python -m etl.flows --windows 1d 7d 30d
python -m etl.concentration --recompute
python -m etl.anomalies --recompute


# Dashboard
streamlit run dashboard/app.py --server.headless true --server.port 8501