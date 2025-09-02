from web3 import Web3
from datetime import datetime
from decimal import Decimal
from psycopg import connect
from psycopg.rows import dict_row
from tenacity import retry, stop_after_attempt, wait_exponential
import os


RPC = os.environ["STORY_RPC_URL"]
w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 60}))


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
def get_block(num):
return w3.eth.get_block(num, full_transactions=True)


def upsert_block(cur, b):
cur.execute(
"""
insert into blocks(number, hash, timestamp)
values (%s,%s,to_timestamp(%s))
on conflict (number) do update set hash=excluded.hash, timestamp=excluded.timestamp
""",
(b.number, bytes.fromhex(b.hash.hex()[2:]), int(b.timestamp))
)


def upsert_tx(cur, tx, success):
cur.execute(
"""
insert into transactions(hash, block_number, from_address, to_address, value_wei, success, gas_used, max_fee_per_gas, max_priority_fee_per_gas)
values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
on conflict (hash) do nothing
""",
(
bytes.fromhex(tx.hash.hex()[2:]),
tx.blockNumber,
bytes.fromhex(tx["from"][2:]),
(bytes.fromhex(tx["to"][2:]) if tx["to"] else None),
int(tx["value"]),
success,
getattr(tx, "gasUsed", None),
getattr(tx, "maxFeePerGas", None),
getattr(tx, "maxPriorityFeePerGas", None),
)
)


def ingest_range(start_block, end_block):
with connect(os.environ.get("DATABASE_URL", f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']} host={os.environ['POSTGRES_HOST']} port={os.environ['POSTGRES_PORT']}"), row_factory=dict_row) as conn:
with conn.cursor() as cur:
for n in range(start_block, end_block+1):
b = get_block(n)
upsert_block(cur, b)
receipts = {}
for tx in b.transactions:
# Fetch receipt to know success
rcpt = w3.eth.get_transaction_receipt(tx.hash)
receipts[tx.hash] = rcpt
upsert_tx(cur, tx, rcpt.status == 1)
if tx.value and tx.value > 0:
cur.execute(
"""
insert into ip_transfers(block_number, tx_hash, idx, from_address, to_address, value_wei, source)
values (%s,%s,%s,%s,%s,%s,'tx')
on conflict do nothing
""",
(n, bytes.fromhex(tx.hash.hex()[2:]), 0, bytes.fromhex(tx["from"][2:]), (bytes.fromhex(tx["to"][2:]) if tx["to"] else None), int(tx.value))
)
conn.commit()