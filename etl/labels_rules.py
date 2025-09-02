import yaml, os
select encode(to_address,'hex') addr, senders, val from fanin where senders > 50 and val > 1e22
""",
"label":"CEX:Hot",
"category":"cex",
"confidence":"medium",
"rationale":"High fan-in from many unique senders"
},
# 2) Vesting: periodic (monthly) outbound to the same cohort set
{
"name":"vesting_periodic",
"sql":
"""
with by_month as (
select date_trunc('month', b.timestamp) m, from_address, count(*) c, sum(value_wei) v
from ip_transfers t join blocks b on b.number=t.block_number
group by 1,2
)
select encode(from_address,'hex') addr, sum(v) v, count(*) months
from by_month group by 1 having months >= 3
""",
"label":"Vesting:Unknown",
"category":"vesting",
"confidence":"low",
"rationale":"Monthly emission pattern"
}
]


if __name__ == "__main__":
with connect(os.environ.get("DATABASE_URL")) as conn:
with conn.cursor() as cur:
# load seeds
for y in SEEDS:
p = os.path.join(os.path.dirname(__file__), y)
if os.path.exists(p):
data = yaml.safe_load(open(p))
for addr, meta in data.items():
cur.execute(
"""
insert into address_labels(address,label,category,confidence,rationale,source)
values (%s,%s,%s,%s,%s,'seed')
on conflict (address) do update set label=excluded.label, category=excluded.category, confidence=excluded.confidence, rationale=excluded.rationale
""",
(bytes.fromhex(addr[2:]), meta["label"], meta["category"], meta.get("confidence","high"), meta.get("rationale","seed"))
)
# run heuristics
for h in HEURISTICS:
cur.execute(h["sql"]) ; rows = cur.fetchall()
for r in rows:
addr = r["addr"]; cur.execute(
"""
insert into address_labels(address,label,category,confidence,rationale,source)
values (%s,%s,%s,%s,%s,'heuristic')
on conflict do nothing
""",
(bytes.fromhex(addr), h["label"], h["category"], h["confidence"], h["rationale"]) )
conn.commit()