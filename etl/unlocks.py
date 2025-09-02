#!/usr/bin/env python3
"""
Generate unlock_schedule rows from config/unlocks.yaml

YAML format (example):
meta:
  total_supply: 1000000000
  tge_date: 2025-02-13
allocations:
  initial_incentives:
    percent: 10.0
    tge_percent: 100.0
  foundation:
    percent: 10.0
    tge_percent: 50.0
    cliff_months: 0
    linear_months: 12
  ecosystem_community:
    percent: 38.4
    tge_amount: 100000000
    cliff_months: 0
    linear_months: 48
  early_backers:
    percent: 21.6
    tge_percent: 0.0
    cliff_months: 12
    linear_months: 36
  core_contributors:
    percent: 20.0
    tge_percent: 0.0
    cliff_months: 12
    linear_months: 36
"""

import os
import sys
import math
import argparse
from datetime import date, timedelta

import yaml
from dateutil.relativedelta import relativedelta
from psycopg import connect
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv()

DBURL = os.environ.get("DATABASE_URL_PG")


def daterange(d0: date, d1: date):
    """Yield dates from d0 to d1 inclusive."""
    d = d0
    while d <= d1:
        yield d
        d += timedelta(days=1)


def parse_yaml(path: str):
    with open(path, "r") as f:
        cfg = yaml.safe_load(f)
    if "meta" not in cfg or "allocations" not in cfg:
        raise ValueError("unlocks.yaml must contain 'meta' and 'allocations' keys")
    meta = cfg["meta"]
    total_supply = float(meta["total_supply"])
    tge_date = date.fromisoformat(str(meta["tge_date"]))
    allocations = cfg["allocations"]
    return total_supply, tge_date, allocations


def upsert_unlock(cur, d, category, amount, basis, note=None):
    cur.execute(
        """
        insert into unlock_schedule (unlock_date, category, amount_ip, basis, note)
        values (%s,%s,%s,%s,%s)
        on conflict (unlock_date, category) do update
        set amount_ip = unlock_schedule.amount_ip + excluded.amount_ip,
            basis = case
                       when unlock_schedule.basis = excluded.basis then unlock_schedule.basis
                       else 'mixed'
                    end,
            note  = coalesce(unlock_schedule.note, excluded.note)
        """,
        (d, category, amount, basis, note),
    )


def main():
    ap = argparse.ArgumentParser(description="Generate unlock_schedule from YAML")
    ap.add_argument("--config", default="config/unlocks.yaml", help="Path to unlocks.yaml")
    ap.add_argument("--truncate", action="store_true", help="Truncate unlock_schedule before inserting")
    args = ap.parse_args()

    if not DBURL:
        print("ERROR: DATABASE_URL_PG not set", file=sys.stderr)
        sys.exit(1)

    total_supply, tge_date, allocations = parse_yaml(args.config)

    with connect(DBURL, row_factory=dict_row) as conn, conn.cursor() as cur:
        if args.truncate:
            cur.execute("truncate table unlock_schedule")

        # Build unlock rows per allocation
        for cat, params in allocations.items():
            # total allocation
            alloc_total = 0.0
            if "amount" in params:
                alloc_total = float(params["amount"])
            elif "percent" in params:
                alloc_total = total_supply * float(params["percent"]) / 100.0
            else:
                raise ValueError(f"Allocation '{cat}' must have 'percent' or 'amount'")

            # TGE unlock
            tge_unlock = 0.0
            if "tge_amount" in params:
                tge_unlock += float(params["tge_amount"])
            if "tge_percent" in params:
                tge_unlock += alloc_total * float(params["tge_percent"]) / 100.0

            # Cap TGE unlock at alloc_total
            tge_unlock = min(tge_unlock, alloc_total)
            remaining = max(alloc_total - tge_unlock, 0.0)

            # Insert TGE row (if any)
            if tge_unlock > 0:
                upsert_unlock(cur, tge_date, cat, tge_unlock, basis="tge", note=None)

            # Linear/cliff setup
            cliff_months = int(params.get("cliff_months", 0) or 0)
            linear_months = int(params.get("linear_months", 0) or 0)

            # If no remaining, continue
            if remaining <= 0.0:
                continue

            # If there is remaining and linear_months == 0 → cliff unlock all at cliff end
            if linear_months == 0:
                unlock_day = tge_date + relativedelta(months=+cliff_months)
                upsert_unlock(cur, unlock_day, cat, remaining, basis="cliff", note=None)
                continue

            # Otherwise, linear vesting starts after cliff
            start = tge_date + relativedelta(months=+cliff_months)
            end = start + relativedelta(months=+linear_months)

            # Distribute 'remaining' evenly per-day (inclusive range)
            # Compute number of days (inclusive)
            days = (end - start).days + 1
            if days <= 0:
                # Fallback: if dates collapse due to month math, unlock at start as cliff
                upsert_unlock(cur, start, cat, remaining, basis="cliff", note="fallback_linear_zero_days")
            else:
                per_day = remaining / days
                # To avoid floating noise, we round to 6 decimals and adjust the last day
                acc = 0.0
                last_day = None
                for i, d in enumerate(daterange(start, end)):
                    amt = round(per_day, 6)
                    acc += amt
                    last_day = d
                    upsert_unlock(cur, d, cat, amt, basis="linear", note=f"linear_{linear_months}m")

                # adjust rounding difference on the last day
                diff = round(remaining - acc, 6)
                if abs(diff) >= 0.000001 and last_day is not None:
                    # update last day row by diff
                    cur.execute(
                        """
                        update unlock_schedule
                        set amount_ip = amount_ip + %s
                        where unlock_date=%s and category=%s
                        """,
                        (diff, last_day, cat),
                    )

        conn.commit()

        # Optional: sanity check total equals declared supply
        cur.execute("select sum(amount_ip) as total from unlock_schedule")
        total_inserted = float(cur.fetchone()["total"] or 0.0)
        # We don't enforce total == supply, because ecosystem TGE_amount etc. may leave some not scheduled (e.g., unassigned buckets).
        print(f"[OK] unlock_schedule generated. Total scheduled amount: {total_inserted:,.6f} IP across categories.")


if __name__ == "__main__":
    main()
