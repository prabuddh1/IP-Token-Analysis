#!/usr/bin/env python3
"""
Build daily supply_timeseries from unlock_schedule + total_supply in YAML.

- circulating_ip = cumulative sum of unlock_schedule.amount_ip (by date)
- locked_ip      = total_supply_ip - circulating_ip
- total_supply_ip is constant from YAML

You can change the definition of "circulating" later (e.g., subtract labeled non-circulating
wallet balances) by editing this script or materializing a view on top.
"""

import os
import sys
import argparse
from datetime import date, timedelta

import yaml
from psycopg import connect
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()
DBURL = os.environ.get("DATABASE_URL_PG")


def parse_yaml_total_and_tge(path: str):
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    meta = cfg.get("meta", {})
    total_supply = float(meta["total_supply"])
    tge_date = date.fromisoformat(str(meta["tge_date"]))
    return total_supply, tge_date


def main():
    ap = argparse.ArgumentParser(description="Build supply_timeseries from unlock_schedule")
    ap.add_argument("--config", default="config/unlocks.yaml", help="Path to unlocks.yaml (for total_supply & tge_date)")
    ap.add_argument("--truncate", action="store_true", help="Truncate supply_timeseries before inserting")
    args = ap.parse_args()

    if not DBURL:
        print("ERROR: DATABASE_URL_PG not set", file=sys.stderr)
        sys.exit(1)

    total_supply, tge_date = parse_yaml_total_and_tge(args.config)

    with connect(DBURL, row_factory=dict_row) as conn, conn.cursor() as cur:
        if args.truncate:
            cur.execute("truncate table supply_timeseries")

        # Get unlock date bounds
        cur.execute("select min(unlock_date) as d0, max(unlock_date) as d1 from unlock_schedule")
        r = cur.fetchone()
        if not r or r["d0"] is None:
            print("ERROR: unlock_schedule is empty. Run etl/unlocks.py first.", file=sys.stderr)
            sys.exit(1)

        start = min(tge_date, r["d0"])
        end = r["d1"]

        # Preload daily unlocks into a dict: date -> amount
        cur.execute("""
            select unlock_date, sum(amount_ip) as amt
            from unlock_schedule
            group by unlock_date
            order by unlock_date
        """)
        by_day = {row["unlock_date"]: float(row["amt"] or 0.0) for row in cur.fetchall()}

        # Build daily cumulative series
        running = 0.0
        d = start
        rows = []
        while d <= end:
            inc = by_day.get(d, 0.0)
            running = round(running + inc, 6)
            circulating = min(running, total_supply)
            locked = max(round(total_supply - circulating, 6), 0.0)
            rows.append((d, total_supply, circulating, locked))
            d += timedelta(days=1)

        # Upsert rows
        cur.executemany(
            """
            insert into supply_timeseries (ts, total_supply_ip, circulating_ip, locked_ip)
            values (%s,%s,%s,%s)
            on conflict (ts) do update
            set total_supply_ip = excluded.total_supply_ip,
                circulating_ip  = excluded.circulating_ip,
                locked_ip       = excluded.locked_ip
            """,
            rows,
        )
        conn.commit()
        print(f"[OK] supply_timeseries upserted for {len(rows)} days: {start} → {end}")


if __name__ == "__main__":
    main()
