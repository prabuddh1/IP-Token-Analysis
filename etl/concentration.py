#!/usr/bin/env python3
import os, argparse
from decimal import Decimal
from psycopg import connect
from psycopg.rows import dict_row

def ensure_schema(cur):
    cur.execute("""
        alter table if exists concentration_timeseries
        add column if not exists gini numeric(10,6)
    """)

def gini(values):
    n = len(values)
    if n < 2: return 0.0
    total = sum(values)
    if total <= 0: return 0.0
    x = sorted(values)
    num = 0.0
    for i, xi in enumerate(x, start=1):
        num += (2*i - n - 1) * xi
    return max(0.0, min(1.0, num / (n * total)))

def compute_one(cur, sampled_at):
    cur.execute("""
        select address, balance_ip
        from balances_latest
        where sampled_at = %s and balance_ip > 0
        order by balance_ip desc
    """, (sampled_at,))
    rows = cur.fetchall()
    if not rows: return None

    bals = [float(Decimal(str(r["balance_ip"]))) for r in rows]
    total = sum(bals)
    top10 = (sum(bals[:10]) / total * 100.0) if total > 0 else 0.0
    top50 = (sum(bals[:50]) / total * 100.0) if total > 0 else 0.0
    hhi   = sum(((b/total)*100.0)**2 for b in bals) if total > 0 else 0.0
    g_pct = gini(bals) * 100.0

    # ts for timeseries = date(sampled_at)
    cur.execute("select (%s)::date as d", (sampled_at,))
    asof = cur.fetchone()["d"]

    # top 50 ranks for that date
    for i, r in enumerate(rows[:50], start=1):
        cur.execute("""
            insert into top_holders_snapshot(asof, rnk, address, balance_ip)
            values (%s,%s,%s,%s)
            on conflict (asof, rnk) do update
            set address=excluded.address, balance_ip=excluded.balance_ip
        """, (asof, i, r["address"], r["balance_ip"]))

    cur.execute("""
        insert into concentration_timeseries(ts, top10_share, top50_share, hhi, gini)
        values (%s,%s,%s,%s,%s)
        on conflict (ts) do update
        set top10_share=excluded.top10_share,
            top50_share=excluded.top50_share,
            hhi=excluded.hhi,
            gini=excluded.gini
    """, (asof, round(top10,4), round(top50,4), round(hhi,4), round(g_pct,4)))
    return asof

def main():
    ap = argparse.ArgumentParser(description="Compute concentration metrics for balances snapshots.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help="Compute for all distinct balances_latest.sampled_at")
    g.add_argument("--date", type=str, help="Compute for a specific YYYY-MM-DD (matching sampled_at::date)")
    args = ap.parse_args()

    DBURL = os.environ.get("DATABASE_URL_PG") or os.environ.get("DATABASE_URL")
    if not DBURL: raise SystemExit("Set DATABASE_URL_PG or DATABASE_URL")

    with connect(DBURL, row_factory=dict_row) as conn, conn.cursor() as cur:
        ensure_schema(cur)
        if args.all:
            cur.execute("select distinct sampled_at from balances_latest order by sampled_at")
            times = [r["sampled_at"] for r in cur.fetchall()]
        else:
            cur.execute("select distinct sampled_at from balances_latest where sampled_at::date = %s::date order by sampled_at", (args.date,))
            times = [r["sampled_at"] for r in cur.fetchall()]

        if not times:
            print("[WARN] No snapshots found for selection.")
            return

        for ts in times:
            asof = compute_one(cur, ts)
            conn.commit()
            if asof:
                print(f"[OK] concentration for {asof}")
        print(f"[DONE] computed {len(times)} day(s).")

if __name__ == "__main__":
    main()
