#!/usr/bin/env python3
import os
from pathlib import Path
from psycopg import connect
from psycopg.rows import dict_row

DBURL = os.environ.get("DATABASE_URL_PG") or os.environ.get("DATABASE_URL")
CEX_FILE = os.environ.get("CEX_SEED_FILE", "config/cex_addresses.txt")

def b(hexaddr: str) -> bytes:
    h = hexaddr.strip()
    return bytes.fromhex(h[2:] if h.startswith("0x") else h)

def seed_cex(cur) -> int:
    p = Path(CEX_FILE)
    if not p.exists():
        print(f"[WARN] {CEX_FILE} not found; skipping CEX seeding.")
        return 0
    n = 0
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(None, 1)
        addr = parts[0]
        tag = parts[1].strip() if len(parts) > 1 else "Exchange"
        cur.execute(
            """
            insert into address_labels(address, label, category, confidence, rationale, source)
            values (%s, %s, 'cex', 'high', 'seed list', 'manual')
            on conflict (address) do update
              set label=excluded.label,
                  category='cex',
                  confidence='high',
                  rationale='seed list',
                  source='manual',
                  updated_at=now()
            """,
            (b(addr), f"CEX:{tag}"),
        )
        n += 1
    return n

def heuristics(cur):
    # Fan-in heavy receivers (possible exchange hot)
    cur.execute(
        """
        with agg as (
          select to_address addr, count(distinct from_address) senders, sum(value_wei) vol
          from ip_transfers
          where to_address is not null
          group by 1
        )
        select addr from agg where senders >= 50 and vol > 1e22
        """
    )
    for r in cur.fetchall():
        cur.execute(
            """
            insert into address_labels(address,label,category,confidence,rationale,source)
            values (%s,'FanIn','unknown','low','many unique senders','heuristic')
            on conflict do nothing
            """,
            (r["addr"],),
        )

    # Fan-out heavy senders (possible redistributors / OTC)
    cur.execute(
        """
        with agg as (
          select from_address addr, count(distinct to_address) receivers, sum(value_wei) vol
          from ip_transfers
          group by 1
        )
        select addr from agg where receivers >= 50 and vol > 1e22
        """
    )
    for r in cur.fetchall():
        cur.execute(
            """
            insert into address_labels(address,label,category,confidence,rationale,source)
            values (%s,'FanOut','unknown','low','many unique receivers','heuristic')
            on conflict do nothing
            """,
            (r["addr"],),
        )

def main():
    if not DBURL:
        raise SystemExit("Set DATABASE_URL_PG or DATABASE_URL")
    with connect(DBURL, row_factory=dict_row) as conn, conn.cursor() as cur:
        seeded = seed_cex(cur)
        heuristics(cur)
        conn.commit()
        print(f"[OK] labels_rules: seeded {seeded} CEX addrs + heuristics applied.")

if __name__ == "__main__":
    main()
