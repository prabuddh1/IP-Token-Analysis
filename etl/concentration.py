from psycopg import connect
import numpy as np


if __name__ == '__main__':
with connect(os.environ.get("DATABASE_URL")) as conn:
with conn.cursor() as cur:
# top-k share & HHI from latest sampled balances
cur.execute("select balance_ip from v_top_holders order by rnk asc")
bals = [float(r[0]) for r in cur.fetchall()]
total = sum(bals)
hhi = sum((b/total*100)**2 for b in bals)
top10 = sum(bals[:10])/total*100 if len(bals)>=10 else None
top50 = sum(bals[:50])/total*100 if len(bals)>=50 else None
print({"hhi":hhi, "top10":top10, "top50":top50})