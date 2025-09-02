import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# ---------- Config ----------
DEFAULT_DB_URL = os.environ.get("DATABASE_URL_PG") or os.environ.get("DATABASE_URL") or ""
st.set_page_config(page_title="Story IP – Supply & Unlocks", layout="wide")

# ---------- Sidebar: DB connection ----------
st.sidebar.header("Database")
db_url = st.sidebar.text_input("Postgres URL", value=DEFAULT_DB_URL, help="e.g. postgresql://user:pass@127.0.0.1:5432/storyip")
if not db_url:
    st.warning("Set DATABASE_URL_PG in your environment or paste the Postgres URL in the sidebar.")
    st.stop()

# Create engine (lazy); keep it simple for now
engine = create_engine(db_url, pool_pre_ping=True)

@st.cache_data(ttl=120)
def q(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, engine)

st.title("Story IP — Supply, Unlocks & Exchange Share (Part-1)")

# =========================================================
# 1) Circulating vs Locked (as-of selector) + Stacked by category
# =========================================================
st.subheader("Circulating vs Locked")

try:
    sup = q("select ts, total_supply_ip, circulating_ip, locked_ip from supply_timeseries order by ts;")
    if sup.empty:
        st.info("`supply_timeseries` is empty. Run `etl/unlocks.py` then `etl/supply.py`.")
    else:
        sup["ts"] = pd.to_datetime(sup["ts"]).dt.date

        # horizon selector
        scope = st.radio("Horizon", ["As of today", "Full schedule", "Custom date"], horizontal=True)

        if scope == "As of today":
            horizon_date = pd.Timestamp.today().date()
        elif scope == "Custom date":
            default_dt = min(max(pd.Timestamp.today().date(), sup["ts"].min()), sup["ts"].max())
            horizon_date = st.date_input("Select date", value=default_dt, min_value=sup["ts"].min(), max_value=sup["ts"].max())
        else:
            horizon_date = sup["ts"].max()  # full schedule through 2029-02-13

        # slice series up to horizon_date for the chart,
        # but for metrics: use the snapshot at horizon (or nearest prior)
        sup_until = sup[sup["ts"] <= horizon_date]
        snap = sup_until.iloc[-1] if not sup_until.empty else sup.iloc[0]

        k1, k2, k3, k4 = st.columns(4)
        circ_pct = 100.0 * float(snap["circulating_ip"]) / float(snap["total_supply_ip"]) if float(snap["total_supply_ip"]) else 0.0
        k1.metric("Total supply (IP)", f"{snap['total_supply_ip']:,.0f}")
        k2.metric("Circulating (IP)", f"{snap['circulating_ip']:,.0f}")
        k3.metric("Locked (IP)", f"{snap['locked_ip']:,.0f}")
        k4.metric("Circulating (%)", f"{circ_pct:,.2f}%")

        st.line_chart(sup_until.set_index("ts")[["circulating_ip", "locked_ip"]])

        # ---------- stacked by allocation (cumulative unlocked) ----------
        st.caption("Cumulative unlocked by allocation (same horizon)")
        # Build cumulative per-category from unlock_schedule
        un = q("""
            select unlock_date::date as ts, category, amount_ip
            from unlock_schedule
            order by ts, category
        """)
        if not un.empty:
            un["ts"] = pd.to_datetime(un["ts"]).dt.date
            un = un[un["ts"] <= horizon_date]
            if not un.empty:
                # daily sums then cumulative
                pivot = (un.pivot_table(index="ts", columns="category", values="amount_ip", aggfunc="sum")
                           .fillna(0.0)
                           .sort_index())
                cum = pivot.cumsum()
                st.area_chart(cum)
            else:
                st.caption("No unlocks up to the selected horizon.")
        else:
            st.caption("`unlock_schedule` is empty; cannot build category stack.")
except Exception as e:
    st.error(f"Failed to load/plot supply/unlocks: {e}")

# =========================================================
# 2) Upcoming unlocks (from unlock_schedule)
# =========================================================
st.subheader("Unlock Schedule (Upcoming)")
try:
    un_up = q("""
        select unlock_date, category, basis, round(amount_ip, 2) as amount_ip
        from unlock_schedule
        where unlock_date >= current_date
        order by unlock_date, category
        limit 200;
    """)
    if un_up.empty:
        st.info("No upcoming unlocks found (either all historical or table empty). Showing recent history instead.")
        un_hist = q("""
            select unlock_date, category, basis, round(amount_ip, 2) as amount_ip
            from unlock_schedule
            order by unlock_date desc, category
            limit 200;
        """)
        st.dataframe(un_hist, use_container_width=True)
    else:
        st.dataframe(un_up, use_container_width=True)
except Exception as e:
    st.error(f"Failed to load unlock_schedule: {e}")

# =========================================================
# 3) Exchange share & per-exchange balances
#    Requires you to label a few CEX addresses in address_labels
#    and refresh the materialized views from sql/views_part1.sql
# =========================================================
st.subheader("Supply on Exchanges (Share of Circulating)")

col_a, col_b = st.columns(2)

with col_a:
    try:
        ex_share = q("""
            select ts, circulating_ip, cex_ip, cex_pct_of_circ
            from mv_exchange_share_daily
            order by ts;
        """)
        if ex_share.empty:
            st.info("`mv_exchange_share_daily` is empty. Seed CEX addresses in `address_labels` and refresh views.")
        else:
            latest = ex_share.iloc[-1]
            st.metric("Latest % of circulating on exchanges", f"{latest['cex_pct_of_circ']:.2f}%")
            st.line_chart(ex_share.set_index("ts")[["cex_pct_of_circ"]])
    except Exception as e:
        st.error(f"Failed to load mv_exchange_share_daily: {e}")

with col_b:
    try:
        ex_bal = q("""
            select ts, exchange, balance_ip
            from mv_exchange_balance_daily
            order by ts;
        """)
        if ex_bal.empty:
            st.info("`mv_exchange_balance_daily` is empty. Label CEX addresses and refresh views.")
        else:
            # Pivot per-exchange balances into columns
            pivot = ex_bal.pivot(index="ts", columns="exchange", values="balance_ip").fillna(0.0)
            st.area_chart(pivot)
    except Exception as e:
        st.error(f"Failed to load mv_exchange_balance_daily: {e}")

# =========================================================
# 4) Post-unlock behavior: sold vs held (7d)
#    -> requires: etl/unlock_attribution.py done (realized_unlocks populated)
#    -> query from sql/unlock_post_event.sql
# =========================================================
st.subheader("Post-Unlock Behavior (Sell-Through in 7 days)")
try:
    post = q("""
        with params as (select 7::int as days),
        ru as (
          select r.unlock_date, r.category, r.dst_address as beneficiary, r.amount_wei,
                 b.timestamp as unlock_ts
          from realized_unlocks r
          join transactions t on t.hash = r.tx_hash
          join blocks b on b.number = t.block_number
        ),
        cex as (select address from address_labels where category='cex'),
        benef_to_cex as (
          select ru.unlock_date, ru.category, ru.beneficiary, sum(it.value_wei) as to_cex_wei
          from ru
          join ip_transfers it on it.from_address = ru.beneficiary
          join blocks b2 on b2.number = it.block_number
          join params p on true
          where it.to_address in (select address from cex)
            and b2.timestamp >= ru.unlock_ts
            and b2.timestamp <  ru.unlock_ts + (p.days || ' days')::interval
          group by 1,2,3
        ),
        benef_net as (
          select ru.unlock_date, ru.category, ru.beneficiary,
                 sum(case when it.to_address   = ru.beneficiary then it.value_wei else 0 end) -
                 sum(case when it.from_address = ru.beneficiary then it.value_wei else 0 end) as net_wei
          from ru
          join ip_transfers it on (it.from_address = ru.beneficiary or it.to_address = ru.beneficiary)
          join blocks b2 on b2.number = it.block_number
          join params p on true
          where b2.timestamp >= ru.unlock_ts
            and b2.timestamp <  ru.unlock_ts + (p.days || ' days')::interval
          group by 1,2,3
        )
        select
          to_char(ru.unlock_date,'YYYY-MM-DD') as unlock_date,
          ru.category,
          '0x'||encode(ru.beneficiary,'hex')   as beneficiary,
          round(ru.amount_wei/1e18, 4)         as unlocked_ip,
          round(coalesce(bc.to_cex_wei,0)/1e18, 4) as to_cex_ip_7d,
          round(100.0*coalesce(bc.to_cex_wei,0)/ru.amount_wei, 4) as sell_through_pct_of_unlock,
          round((coalesce(ru.amount_wei,0) + coalesce(benef_net.net_wei,0))/1e18, 4) as held_or_net_change_ip_7d
        from ru
        left join benef_to_cex bc on (bc.unlock_date,bc.category,bc.beneficiary)=(ru.unlock_date,ru.category,ru.beneficiary)
        left join benef_net       on (benef_net.unlock_date,benef_net.category,benef_net.beneficiary)=(ru.unlock_date,ru.category,ru.beneficiary)
        order by ru.unlock_date desc, ru.category;
    """)
    if post.empty:
        st.info("`realized_unlocks` appears empty. Run:\n\n`python etl/unlock_attribution.py --pre-days 1 --post-days 1 --min-ip 25000`\n\nThen refresh this page.")
    else:
        st.dataframe(post, use_container_width=True)
except Exception as e:
    st.error(f"Failed to compute post-unlock behavior: {e}")

st.caption("Tip: Update `address_labels` (CEX / treasury / vesting) and refresh the materialized views to improve accuracy.")
