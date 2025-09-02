import os, time
from dotenv import load_dotenv
from psycopg import connect
from web3 import Web3


load_dotenv()
w3 = Web3(Web3.HTTPProvider(os.environ["STORY_RPC_URL"], request_kwargs={"timeout": 60}))


BATCH = 100


def addresses_to_sample(conn):
    with conn.cursor() as cur:
        cur.execute("""
        with cands as (
            select distinct addr from (
                select to_address as addr from ip_transfers where to_address is not null
                union all
                select from_address as addr from ip_transfers
            ) u
        )
        select encode(addr,'hex') from cands limit 1000
        """)
        return ["0x"+r[0] for r in cur.fetchall()]


if __name__ == "__main__":
    now = int(time.time())
    with connect(os.environ["DATABASE_URL_PG"].replace("+psycopg","")) as conn:
        addrs = addresses_to_sample(conn)
        blk = w3.eth.block_number
        with conn.cursor() as cur:
            for a in addrs:
                bal = w3.eth.get_balance(a, blk)/1e18
                cur.execute("""
                insert into balances_latest(sampled_at, block_number, address, balance_ip)
                values (to_timestamp(%s), %s, %s, %s)
                """, (now, blk, bytes.fromhex(a[2:]), bal))
            conn.commit()
        print(f"Sampled {len(addrs)} balances at block {blk}")