#!/usr/bin/env python3
"""
Backfill Story mainnet native IP flows from block ranges or the last N days.

Writes to Postgres:
  - blocks, transactions
  - traces_value (internal value transfers, optional)
  - ip_transfers (unified native value movements: source='tx' | 'trace')
  - sync_state (checkpointing)

Env (set in .env then `set -a; source .env; set +a`):
  STORY_RPC_URL   = https://<your-quicknode-or-provider-endpoint>
  DATABASE_URL_PG = postgresql://user:pass@host:5432/storyip

Examples:
  # Fast Tier-1 sweep (no receipts, no traces)
  python scripts/backfill.py --days 30 --batch-blocks 4000 --no-receipts --concurrency 12 --sleep 0 --log-every 2000

  # Targeted traces around an unlock (±few days)
  python scripts/backfill.py --days 10 --with-traces --fallback-debug --batch-blocks 1000 --sleep 0.05 --no-receipts --concurrency 8

  # Explicit block range
  python scripts/backfill.py --start-block 2_000_000 --end-block 2_050_000 --batch-blocks 4000 --no-receipts --concurrency 12
"""
import argparse
import os
import sys
import time
import random
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from tenacity import retry, stop_after_attempt, wait_exponential
from psycopg import connect
from psycopg.rows import dict_row
from web3 import Web3

RPC = os.environ.get("STORY_RPC_URL")
DBURL = os.environ.get("DATABASE_URL_PG")
if not RPC or not DBURL:
    print("ERROR: Set STORY_RPC_URL and DATABASE_URL_PG in your environment (.env).", file=sys.stderr)
    sys.exit(1)

# ---------- HTTP Session (keep-alive) with basic pool ----------
SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})
# generous pool; provider cap is enforced via backoff below
adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=Retry(total=0))
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

# ---------- Web3 for convenience (full tx decoding) ----------
w3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": 90}))

# ----------------------------- RPC helpers -----------------------------
def _rpc(method, params):
    """JSON-RPC with 429 backoff + jitter; accepts text/plain on provider errors."""
    for attempt in range(6):
        r = SESSION.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=90)
        if r.status_code == 429:
            time.sleep(min(1.0 * (2 ** attempt), 5.0) + random.random() * 0.2)
            continue
        r.raise_for_status()
        # some providers reply text/plain on overload; let json() raise if truly invalid
        j = r.json()
        if "error" in j:
            # backoff on server-overloaded style errors, else raise
            if j["error"].get("code") in (-32005, -32603):
                time.sleep(min(1.0 * (2 ** attempt), 5.0))
                continue
            raise RuntimeError(f"RPC error: {j['error']}")
        return j["result"]
    raise RuntimeError("RPC 429/backoff exceeded")

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
def get_block(num: int):
    # Use web3 for full tx decode (full_transactions=True)
    return w3.eth.get_block(num, full_transactions=True)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=10))
def get_receipt(tx_hash):
    return w3.eth.get_transaction_receipt(tx_hash)

def trace_block_supported() -> bool:
    try:
        _ = _rpc("trace_block", [hex(w3.eth.block_number)])
        return True
    except Exception:
        return False

def debug_trace_tx(tx_hash_hex: str):
    params = [tx_hash_hex, {"tracer": "callTracer"}]
    return _rpc("debug_traceTransaction", params)

# ----------------------------- DB bootstrap -----------------------------
SCHEMA_MIN = """
create table if not exists blocks (
  number bigint primary key,
  hash bytea unique,
  timestamp timestamptz not null
);

create table if not exists transactions (
  hash bytea primary key,
  block_number bigint not null references blocks(number),
  from_address bytea not null,
  to_address bytea,
  value_wei numeric(78,0) not null,
  success boolean,
  gas_used bigint,
  max_fee_per_gas numeric(38,0),
  max_priority_fee_per_gas numeric(38,0)
);

create table if not exists traces_value (
  tx_hash bytea not null references transactions(hash),
  trace_index int not null,
  from_address bytea not null,
  to_address bytea,
  value_wei numeric(78,0) not null,
  primary key (tx_hash, trace_index)
);

create table if not exists ip_transfers (
  block_number bigint not null,
  tx_hash bytea not null,
  idx int not null,                -- 0 for tx.value; >0 for trace steps
  from_address bytea not null,
  to_address bytea,
  value_wei numeric(78,0) not null,
  source text not null,            -- 'tx' | 'trace'
  primary key (tx_hash, idx)
);

create table if not exists sync_state (
  id text primary key,
  last_block bigint,
  updated_at timestamptz default now()
);

create index if not exists idx_ip_block on ip_transfers(block_number);
create index if not exists idx_ip_from  on ip_transfers(from_address);
create index if not exists idx_ip_to    on ip_transfers(to_address);
"""

def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_MIN)
        # ensure success is nullable for --no-receipts speed mode
        cur.execute("""
            do $$
            begin
              if exists (
                select 1 from information_schema.columns
                where table_name='transactions' and column_name='success' and is_nullable='NO'
              ) then
                execute 'alter table transactions alter column success drop not null';
              end if;
            end$$;
        """)
    conn.commit()

def set_checkpoint(conn, last_block: int, sync_id: str):
    with conn.cursor() as cur:
        cur.execute("""
          insert into sync_state(id, last_block) values (%s,%s)
          on conflict (id) do update set last_block=excluded.last_block, updated_at=now()
        """, (sync_id, last_block))
    conn.commit()

def get_checkpoint(conn, sync_id: str) -> Optional[int]:
    with conn.cursor() as cur:
        cur.execute("select last_block from sync_state where id=%s", (sync_id,))
        r = cur.fetchone()
        return r[0] if r else None

# ----------------------------- Helpers -----------------------------
def block_ts(num: int) -> int:
    return int(get_block(num).timestamp)

def find_start_block_from_days(days: int, tip: int) -> int:
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp())
    step = 5000
    n = tip
    while n > 0:
        ts = block_ts(n)
        if ts <= cutoff:
            low, high = max(0, n - step), n
            while low < high:
                mid = (low + high) // 2
                if block_ts(mid) < cutoff:
                    low = mid + 1
                else:
                    high = mid
            return max(0, low - 1)
        n = max(0, n - step)
    return 0

def b(hexstr: Optional[str]) -> Optional[bytes]:
    if hexstr is None:
        return None
    return bytes.fromhex(hexstr[2:]) if hexstr.startswith("0x") else bytes.fromhex(hexstr)

def upsert_block(cur, bobj):
    cur.execute(
        "insert into blocks(number, hash, timestamp) values (%s,%s,to_timestamp(%s)) "
        "on conflict (number) do update set hash=excluded.hash, timestamp=excluded.timestamp",
        (bobj.number, b(bobj.hash.hex()), int(bobj.timestamp))
    )

def upsert_tx(cur, tx, rcpt):
    success = None if rcpt is None else (rcpt.status == 1)
    gas_used = None if rcpt is None else getattr(rcpt, "gasUsed", None)
    cur.execute(
        """
        insert into transactions(hash, block_number, from_address, to_address, value_wei, success, gas_used,
                                 max_fee_per_gas, max_priority_fee_per_gas)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on conflict (hash) do nothing
        """,
        (
            b(tx.hash.hex()),
            tx.blockNumber,
            b(tx["from"]),
            (b(tx["to"]) if tx["to"] else None),
            int(tx["value"]),
            success,
            gas_used,
            getattr(tx, "maxFeePerGas", None),
            getattr(tx, "maxPriorityFeePerGas", None),
        )
    )

def insert_tx_value_transfer(cur, block_num: int, tx_hash_hex: str, from_addr: str, to_addr: Optional[str], value_wei: int):
    if value_wei and value_wei > 0:
        cur.execute(
            """
            insert into ip_transfers(block_number, tx_hash, idx, from_address, to_address, value_wei, source)
            values (%s,%s,%s,%s,%s,%s,'tx')
            on conflict do nothing
            """,
            (block_num, b(tx_hash_hex), 0, b(from_addr), (b(to_addr) if to_addr else None), int(value_wei))
        )

def insert_trace_value(cur, block_num: int, tx_hash_hex: str, idx: int, from_addr: str, to_addr: Optional[str], value_wei: int):
    cur.execute(
        "insert into traces_value(tx_hash, trace_index, from_address, to_address, value_wei) values (%s,%s,%s,%s,%s) on conflict do nothing",
        (b(tx_hash_hex), idx, b(from_addr), (b(to_addr) if to_addr else None), int(value_wei))
    )
    cur.execute(
        "insert into ip_transfers(block_number, tx_hash, idx, from_address, to_address, value_wei, source) values (%s,%s,%s,%s,%s,%s,'trace') on conflict do nothing",
        (block_num, b(tx_hash_hex), idx, b(from_addr), (b(to_addr) if to_addr else None), int(value_wei))
    )

def iter_value_traces_from_debug_call(call):
    try:
        val = int(call.get("value", "0x0"), 16)
    except Exception:
        val = 0
    if val and val > 0:
        yield (call.get("from"), call.get("to"), val)
    for sub in call.get("calls", []) or []:
        yield from iter_value_traces_from_debug_call(sub)

def ingest_traces_for_block(cur, block_num: int, tx_hashes: List[str], prefer_trace_block: bool, sleep_between_calls: float, fallback_debug: bool):
    if prefer_trace_block:
        try:
            traces = _rpc("trace_block", [hex(block_num)])
            for t in traces or []:
                act = t.get("action", {})
                val_hex = act.get("value")
                if not val_hex:
                    continue
                val = int(val_hex, 16)
                if val <= 0:
                    continue
                frm = act.get("from")
                to = act.get("to")
                txh = t.get("transactionHash")
                idx = len(t.get("traceAddress", []) or [])
                insert_trace_value(cur, block_num, txh, idx, frm, to, val)
            return
        except Exception:
            if not fallback_debug:
                raise
            # fall through

    for txh in tx_hashes:
        try:
            res = debug_trace_tx(txh)
            i = 1
            for frm, to, val in iter_value_traces_from_debug_call(res):
                insert_trace_value(cur, block_num, txh, i, frm, to, val)
                i += 1
        except Exception:
            pass
        if sleep_between_calls:
            time.sleep(sleep_between_calls)

# ----------------------------- Progress utils -----------------------------
def fmt_duration(seconds: float) -> str:
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h: return f"{h}h {m}m {s}s"
    if m: return f"{m}m {s}s"
    return f"{s}s"

def progress_line(done: int, total: int, elapsed: float) -> str:
    rate = done / elapsed if elapsed > 0 else 0.0
    eta = (total - done) / rate if rate > 0 else 0.0
    return f"{done}/{total} blocks | {rate:.2f} blk/s | elapsed {fmt_duration(elapsed)} | ETA {fmt_duration(eta)}"

# ----------------------------- Main processing -----------------------------
def process_batch(conn, start_block: int, end_block: int, with_traces: bool, sleep_s: float,
                  prefer_traceblock: bool, fallback_debug: bool, no_receipts: bool,
                  log_every: int, concurrency: int):
    """Inclusive batch: [start_block, end_block]. Commit once per batch."""
    total = end_block - start_block + 1
    t0 = time.time()
    done = 0

    with conn.cursor() as cur, ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = {ex.submit(get_block, n): n for n in range(start_block, end_block + 1)}
        for fut in as_completed(futures):
            bobj = fut.result()
            n = bobj.number
            upsert_block(cur, bobj)

            tx_hashes = []
            for tx in bobj.transactions:
                rcpt = None if no_receipts else get_receipt(tx.hash)
                upsert_tx(cur, tx, rcpt)
                txh = tx.hash.hex()
                tx_hashes.append(txh)
                if int(tx["value"]) > 0:
                    insert_tx_value_transfer(cur, n, txh, tx["from"], tx["to"], int(tx["value"]))

            if with_traces:
                ingest_traces_for_block(
                    cur,
                    n,
                    tx_hashes,
                    prefer_trace_block=prefer_traceblock,
                    sleep_between_calls=sleep_s,
                    fallback_debug=fallback_debug,
                )

            done += 1
            if log_every and (done % log_every == 0):
                now = time.time()
                print(f"[PROGRESS] {progress_line(done, total, now - t0)}", flush=True)

    conn.commit()
    print(f"[PROGRESS] {progress_line(total, total, time.time() - t0)}", flush=True)

def main():
    ap = argparse.ArgumentParser(description="Story IP backfill (blocks, txs, traces -> Postgres)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--days", type=int, help="Backfill the last N days (approx).")
    g.add_argument("--start-block", type=int, help="Start block number (inclusive).")
    ap.add_argument("--end-block", type=int, help="End block number (inclusive). If omitted with --days, uses tip.")
    ap.add_argument("--batch-blocks", type=int, default=500, help="Batch size (blocks) for the outer loop.")
    ap.add_argument("--with-traces", action="store_true", help="Also ingest internal value transfers (trace_block/debug).")
    ap.add_argument("--no-traceblock", action="store_true", help="Disable trace_block even if available (force debug_trace).")
    ap.add_argument("--fallback-debug", action="store_true", help="If trace_block fails, fall back to debug_traceTransaction.")
    ap.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between trace calls (to respect rate limits).")
    ap.add_argument("--resume", action="store_true", help="Resume from last checkpoint in sync_state (id='main').")
    ap.add_argument("--no-receipts", action="store_true", help="Skip tx receipts (faster Tier-1).")
    ap.add_argument("--log-every", type=int, default=500, help="Log progress every N blocks (within a batch).")
    ap.add_argument("--concurrency", type=int, default=12, help="Number of concurrent block fetches")
    args = ap.parse_args()

    tip = w3.eth.block_number

    # Compute range
    if args.days is not None:
        start = find_start_block_from_days(args.days, tip)
        end = args.end_block if args.end_block is not None else tip
    else:
        start = args.start_block
        end = args.end_block if args.end_block is not None else tip

    if start > end:
        print("ERROR: start block must be <= end block", file=sys.stderr)
        sys.exit(1)

    prefer_traceblock = (not args.no_traceblock) and trace_block_supported()
    if args.with_traces:
        print(f"[INFO] Traces enabled. trace_block supported? {prefer_traceblock}")

    with connect(DBURL, row_factory=dict_row) as conn:
        ensure_schema(conn)

        sync_id = "main"
        if args.resume:
            ck = get_checkpoint(conn, sync_id)
            if ck is not None and ck >= start:
                start = ck + 1
                print(f"[INFO] Resuming from checkpoint: next start_block={start}")

        total_blocks = end - start + 1
        if total_blocks <= 0:
            print("[INFO] Nothing to do (range already synced).")
            return

        print(f"[INFO] Backfilling blocks [{start} .. {end}] (total {total_blocks}) | "
              f"batch={args.batch_blocks} | receipts={'no' if args.no_receipts else 'yes'} | "
              f"traces={'yes' if args.with_traces else 'no'} | sleep={args.sleep} | "
              f"concurrency={args.concurrency}")

        try:
            for batch_start in range(start, end + 1, args.batch_blocks):
                batch_end = min(end, batch_start + args.batch_blocks - 1)
                t0 = time.time()
                try:
                    process_batch(
                        conn,
                        batch_start,
                        batch_end,
                        with_traces=args.with_traces,
                        sleep_s=args.sleep,
                        prefer_traceblock=prefer_traceblock,
                        fallback_debug=args.fallback_debug,
                        no_receipts=args.no_receipts,
                        log_every=args.log_every,
                        concurrency=args.concurrency,
                    )
                    set_checkpoint(conn, batch_end, sync_id)
                except Exception as e:
                    conn.rollback()  # important: clean the tx
                    set_checkpoint(conn, batch_start - 1, sync_id)
                    print(f"[ERROR] Batch {batch_start}-{batch_end} failed: {e}", file=sys.stderr)
                    raise
                dt = time.time() - t0
                print(f"[OK] Synced {batch_start}-{batch_end} in {dt:.1f}s; checkpoint={batch_end}", flush=True)
        except KeyboardInterrupt:
            print("[INFO] Interrupted by user, saving checkpoint and exiting.")
        finally:
            conn.commit()

if __name__ == "__main__":
    main()
