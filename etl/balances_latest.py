#!/usr/bin/env python3
"""
Backfill balances snapshots from ip_transfers and write:
  - balances_latest  (one row per (sampled_at, address))
  - top_holders_snapshot (top N per day)

Usage:
  python etl/balances_latest.py --days 10        # last 10 days incl. today
  python etl/balances_latest.py --days 1         # today only (default)

Env:
  DATABASE_URL_PG / DATABASE_URL   Postgres URL
  TOPN_HOLDERS                     default 200
"""

import os, argparse
from datetime import datetime, timedelta, timezone
from psycopg import connect
from psycopg.rows import dict_row

TOPN = int(os.environ.get("TOPN_HOLDERS", "200"))

def end_of_day_block(cur, day_utc):
    """
    Return the max block whose timestamp < (day_utc + 1 day).
    day_utc is a date (no time) in UTC.
    """
    cur.execute("""
        select max(number) as n
        from blocks
        where timestamp < (%s::timestamptz + interval '1 day')
    """, (datetime.combine(day_utc, datetime.min.time()).replace(tzinfo=timezone.utc).isoformat(),))
    r = cur.fetchone()
    return r["n"]

def upsert_snapshot(cur, sampled_at_ts, upto_block):
    """
    Compute balances up to block <= upto_block and insert a snapshot at sampled_at_ts.
    """
    cur.execute("""
        with x as (
          select t.from_address, t.to_address, t.value_wei
          from ip_transfers t
          where t.block_number <= %s
        ),
        inflow as (
          select to_address as addr, sum(value_wei) as in_wei
          from x where to_address is not null group by 1
        ),
        outflow as (
          select from_address as addr, sum(value_wei) as out_wei
          from x group by 1
        ),
        net as (
          select coalesce(i.addr, o.addr) as addr,
                 coalesce(i.in_wei,0) - coalesce(o.out_wei,0) as net_wei
          from inflow i
          full outer join outflow o on i.addr=o.addr
        )
        insert into balances_latest(sampled_at, block_number, address, balance_ip)
        select %s::timestamptz, %s::bigint, addr, (net_wei/1e18)::numeric
        from net
        where net_wei is not null and net_wei <> 0
        on conflict (sampled_at, address) do update
        set block_number = excluded.block_number,
            balance_ip  = excluded.balance_ip
    """, (upto_block, sampled_at_ts, upto_block))

def write_top_holders(cur, asof_date, sampled_at_ts):
    # Clear any existing snapshot for this date (idempotent, avoids conflicts)
    cur.execute("delete from top_holders_snapshot where asof = %s", (asof_date,))

    cur.execute("""
        with latest as (
          select address, balance_ip
          from balances_latest
          where sampled_at = %s
        ),
        ranked as (
          select
            address,
            balance_ip,
            row_number() over (
              order by balance_ip desc, address asc
            ) as rnum
          from latest
        )
        insert into top_holders_snapshot(asof, rnk, address, balance_ip)
        select %s::date, rnum, address, balance_ip
        from ranked
        where rnum <= %s
    """, (sampled_at_ts, asof_date, TOPN))


def main():
    ap = argparse.ArgumentParser(description="Backfill balances_latest snapshots (one per day).")
    ap.add_argument("--days", type=int, default=1, help="How many days back (inclusive of today).")
    args = ap.parse_args()

    DBURL = os.environ.get("DATABASE_URL_PG") or os.environ.get("DATABASE_URL")
    if not DBURL:
        raise SystemExit("Set DATABASE_URL_PG or DATABASE_URL")

    with connect(DBURL, row_factory=dict_row) as conn, conn.cursor() as cur:
        today = datetime.now(timezone.utc).date()
        total_days = 0
        for d in range(args.days):
            day = today - timedelta(days=d)

            upto_block = end_of_day_block(cur, day)
            if not upto_block:
                print(f"[SKIP] No block before {day} 23:59:59Z")
                continue

            sampled_at_ts = datetime.combine(day, datetime.max.time()).replace(tzinfo=timezone.utc)

            upsert_snapshot(cur, sampled_at_ts, upto_block)
            write_top_holders(cur, day, sampled_at_ts)
            conn.commit()

            # quick stats
            cur.execute("select count(*) c from balances_latest where sampled_at=%s", (sampled_at_ts,))
            c = cur.fetchone()["c"]
            print(f"[OK] snapshot {day} (UTC) @ block {upto_block} -> {c} rows")
            total_days += 1

        print(f"[DONE] inserted/updated {total_days} day(s).")

if __name__ == "__main__":
    main()
