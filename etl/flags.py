#!/usr/bin/env python3
"""
Populate holder_flags table with heuristics:
- NEVER_SOLD
- CONSISTENT_SELLER (post-unlock window)
- ACCUMULATOR (net inflow > 1M IP over 30d)
- REDISTRIBUTOR (fan-out to 50+ addrs in 30d)
"""

import os
from psycopg import connect

DBURL = os.environ.get("DATABASE_URL_PG") or os.environ.get("DATABASE_URL")
POST_UNLOCK_WINDOW_DAYS = 3  # +/- measured as [unlock_date, unlock_date+3d]

SQL_NEVER_SOLD = """
insert into holder_flags(asof, address, flag, time_window, confidence, rationale)
select current_date, addr, 'NEVER_SOLD', 'alltime', 'medium',
       'Address received IP but has no outbound transfers'
from (
  -- all recipients (non-null)
  select distinct to_address as addr
  from ip_transfers
  where to_address is not null

  except

  -- any address that has ever sent (non-null)
  select distinct from_address
  from ip_transfers
  where from_address is not null
) x
where addr is not null
on conflict do nothing;
"""

SQL_CONSISTENT_SELLER = f"""
with cex as (
  select address from address_labels where category='cex'
), hits as (
  select t.from_address as addr, count(*) as k
  from ip_transfers t
  join blocks b on b.number = t.block_number
  where t.from_address is not null
    and t.to_address   in (select address from cex)
    and exists (
      select 1 from unlock_schedule u
      where b.timestamp::date
        between u.unlock_date and u.unlock_date + make_interval(days => {POST_UNLOCK_WINDOW_DAYS})
    )
  group by 1
)
insert into holder_flags(asof, address, flag, time_window, confidence, rationale)
select current_date, addr, 'CONSISTENT_SELLER', 'post-unlock:{POST_UNLOCK_WINDOW_DAYS}d',
       'low', 'Outflows to CEX within 3 days of multiple unlocks'
from hits
where addr is not null and k >= 2
on conflict do nothing;
"""

SQL_ACCUMULATOR = """
with recent as (
  select t.from_address, t.to_address, t.value_wei
  from ip_transfers t
  join blocks b on b.number = t.block_number
  where b.timestamp >= now() - make_interval(days => 30)
),
ins as (
  select to_address as addr, sum(value_wei)/1e18 as in_ip
  from recent
  where to_address is not null
  group by 1
),
outs as (
  select from_address as addr, sum(value_wei)/1e18 as out_ip
  from recent
  where from_address is not null
  group by 1
),
net as (
  select coalesce(ins.addr, outs.addr) as addr,
         coalesce(in_ip,0) - coalesce(out_ip,0) as net_ip
  from ins full outer join outs on outs.addr = ins.addr
)
insert into holder_flags(asof, address, flag, time_window, confidence, rationale)
select current_date, addr, 'ACCUMULATOR', '30d', 'medium',
       'Net inflow over last 30d exceeds 1M IP'
from net
where addr is not null and net_ip > 1e6
on conflict do nothing;
"""

SQL_REDISTRIBUTOR = """
with recent as (
  select t.from_address, t.to_address, t.value_wei
  from ip_transfers t
  join blocks b on b.number = t.block_number
  where b.timestamp >= now() - make_interval(days => 30)
),
fanout as (
  select from_address as addr,
         count(distinct to_address) as rcnt,
         sum(value_wei)/1e18 as v
  from recent
  where from_address is not null and to_address is not null
  group by 1
)
insert into holder_flags(asof, address, flag, time_window, confidence, rationale)
select current_date, addr, 'REDISTRIBUTOR', '30d', 'low',
       'High fan-out to many recipients in last 30d'
from fanout
where addr is not null and rcnt >= 50 and v > 1e6
on conflict do nothing;
"""

def main():
    if not DBURL:
        raise SystemExit("Set DATABASE_URL_PG or DATABASE_URL")
    with connect(DBURL) as conn, conn.cursor() as cur:
        cur.execute(SQL_NEVER_SOLD)
        cur.execute(SQL_CONSISTENT_SELLER)
        cur.execute(SQL_ACCUMULATOR)
        cur.execute(SQL_REDISTRIBUTOR)
        conn.commit()
        print("[OK] holder_flags updated")

if __name__ == "__main__":
    main()
