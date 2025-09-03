#!/usr/bin/env python3
import os
from psycopg import connect
from psycopg.rows import dict_row

DBURL = os.environ.get("DATABASE_URL_PG") or os.environ.get("DATABASE_URL")

# Hardcoded intervals (safe to format directly)
WINDOWS = {
    "1d": "1 day",
    "3d": "3 days",
    "7d": "7 days",
    "30d": "30 days",
}

SQL_BASE_TMPL = """
with x as (
  select b.timestamp::date as d, t.from_address, t.to_address, t.value_wei
  from ip_transfers t
  join blocks b on b.number = t.block_number
  where b.timestamp >= now() - interval '{win}'
),
ex as (
  select address from address_labels where category='cex'
)
select
  d,
  sum(case when to_address   in (select address from ex) then value_wei else 0 end)/1e18 as to_cex_ip,
  sum(case when from_address in (select address from ex) then value_wei else 0 end)/1e18 as from_cex_ip
from x
group by d
order by d;
"""

SQL_UNLOCK_PROX = """
with near as (
  select 1 as hit
  from unlock_schedule
  where unlock_date between %s::date - interval '1 day' and %s::date + interval '1 day'
  limit 1
)
select case when exists(select 1 from near) then 'near-unlock' else 'none' end as prox;
"""

def main():
    if not DBURL:
        raise SystemExit("Set DATABASE_URL_PG or DATABASE_URL")
    with connect(DBURL, row_factory=dict_row) as conn, conn.cursor() as cur:
        for key, win in WINDOWS.items():
            sql = SQL_BASE_TMPL.format(win=win)  # safe: win comes from constant dict
            cur.execute(sql)
            rows = cur.fetchall()
            if not rows:
                print(f"[INFO] {key}: no rows")
                continue

            inserted = 0
            for r in rows:
                d = r["d"]
                to_cex = float(r["to_cex_ip"] or 0.0)
                from_cex = float(r["from_cex_ip"] or 0.0)
                net = to_cex - from_cex

                # unlock proximity tag for the day
                cur.execute(SQL_UNLOCK_PROX, (d, d))
                prox = cur.fetchone()["prox"]

                cur.execute(
                    """
                    insert into exchange_flows(time_window, asof, exchange, net_in_ip, unlock_proximity)
                    values (%s,%s,%s,%s,%s)
                    on conflict (time_window, asof, exchange)
                    do update set net_in_ip = excluded.net_in_ip,
                                  unlock_proximity = excluded.unlock_proximity
                    """,
                    (key, d, "ALL", net, prox),
                )
                inserted += 1

            conn.commit()
            print(f"[OK] exchange_flows updated for {key}: {inserted} day rows")

if __name__ == "__main__":
    main()
