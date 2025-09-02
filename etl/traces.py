import os, json
import requests
from psycopg import connect
from psycopg.rows import dict_row


RPC = os.environ["STORY_RPC_URL"]


def _rpc(method, params):
r = requests.post(RPC, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=60)
r.raise_for_status()
return r.json()["result"]


# Prefer trace_block (Alchemy/QuickNode). Fallback to per-tx debug_traceTransaction.


def ingest_traces_for_block(block_num):
try:
traces = _rpc("trace_block", [hex(block_num)])
except Exception:
# fallback: loop receipts
return
rows = []
for t in traces:
if t.get("action", {}).get("value") and int(t["action"]["value"],16) > 0:
frm = t["action"].get("from")
to = t["action"].get("to")
val = int(t["action"]["value"],16)
txh = t.get("transactionHash")
idx = t.get("traceAddress", [])
rows.append((txh, len(idx), frm, to, val))
if not rows:
return
with connect(os.environ.get("DATABASE_URL", f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']} host={os.environ['POSTGRES_HOST']} port={os.environ['POSTGRES_PORT']}"), row_factory=dict_row) as conn:
with conn.cursor() as cur:
for txh, i, frm, to, val in rows:
cur.execute(
"""
insert into traces_value(tx_hash, trace_index, from_address, to_address, value_wei)
values (%s,%s,%s,%
s,%s) on conflict do nothing
""",
(bytes.fromhex(txh[2:]), i, bytes.fromhex(frm[2:]), (bytes.fromhex(to[2:]) if to else None), val)
)
cur.execute(
"""
insert into ip_transfers(block_number, tx_hash, idx, from_address, to_address, value_wei, source)
select t.block_number, %s, %s, %s, %s, %s, 'trace' from transactions t where t.hash=%s
on conflict do nothing
""",
(bytes.fromhex(txh[2:]), i, bytes.fromhex(frm[2:]), (bytes.fromhex(to[2:]) if to else None), val, bytes.fromhex(txh[2:]))
)
conn.commit()