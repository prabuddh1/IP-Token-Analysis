from psycopg import connect
import pandas as pd
import os


WINDOWS = {
'1d': '1 day',
'3d': '3 days',
'7d': '7 days',
'30d': '30 days'
}


EXCH = "select address from address_labels where category='cex'"


SQL = """
with x as (
select to_timestamp(extract(epoch from b.timestamp)) as ts, t.*
from ip_transfers t join blocks b on b.number=t.block_number
), ex as ({exch})
select date_trunc('day', ts)::date as d,
case when to_address in (select address from ex) then 'to_cex'
when from_address in (select address from ex) then 'from_cex'
else 'other' end as dir,
sum(value_wei)/1e18 as ip
from x
where ts >= now() - interval '{win}'
and (to_address in (select address from ex) or from_address in (select address from ex))
group by 1,2
"""


if __name__ == '__main__':
with connect(os.environ.get("DATABASE_URL")) as conn:
for k, win in WINDOWS.items():
df = pd.read_sql_query(SQL.format(exch=EXCH, win=win), conn)
# write into exchange_flows/net
for d, grp in df.groupby('d'):
net = float(grp.loc[grp['dir']=='to_cex','ip'].sum() - grp.loc[grp['dir']=='from_cex','ip'].sum())
with conn.cursor() as cur:
cur.execute("""
insert into exchange_flows(window, asof, exchange, net_in_ip, unlock_proximity)
values (%s,%s,%s,%s,%s)
on conflict (window, asof, exchange) do update set net_in_ip=excluded.net_in_ip
""", (k, d, 'ALL', net, 'none'))
conn.commit()