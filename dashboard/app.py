# dashboard/app.py
import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# ---------- Page / DB config ----------
st.set_page_config(page_title="Story IP — Supply, Unlocks & Holders", layout="wide")
DEFAULT_DB_URL = os.environ.get("DATABASE_URL_PG") or os.environ.get("DATABASE_URL") or ""

st.sidebar.header("Database")
db_url = st.sidebar.text_input(
    "Postgres URL",
    value=DEFAULT_DB_URL,
    help="E.g. postgresql://user:pass@127.0.0.1:5432/storyip",
)
if not db_url:
    st.warning("Set DATABASE_URL_PG in your environment or paste the Postgres URL in the sidebar.")
    st.stop()

engine = create_engine(db_url, pool_pre_ping=True)

@st.cache_data(ttl=120)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)

st.title("Story IP — Supply, Unlocks & Holders")

# =========================================================
# 1) Circulating vs Locked (as-of selector) + category stack
# =========================================================
st.subheader("Circulating vs Locked")
try:
    sup = q("select ts, total_supply_ip, circulating_ip, locked_ip from supply_timeseries order by ts;")
    if sup.empty:
        st.info("`supply_timeseries` is empty. Run `etl/unlocks.py` then `etl/supply.py`.")
    else:
        sup["ts"] = pd.to_datetime(sup["ts"]).dt.date
        scope = st.radio("Horizon", ["As of today", "Full schedule", "Custom date"], horizontal=True)

        if scope == "As of today":
            horizon_date = pd.Timestamp.today().date()
        elif scope == "Custom date":
            default_dt = min(max(pd.Timestamp.today().date(), sup["ts"].min()), sup["ts"].max())
            horizon_date = st.date_input(
                "Select date",
                value=default_dt,
                min_value=sup["ts"].min(),
                max_value=sup["ts"].max(),
            )
        else:
            horizon_date = sup["ts"].max()  # full modeled schedule

        sup_until = sup[sup["ts"] <= horizon_date]
        snap = sup_until.iloc[-1] if not sup_until.empty else sup.iloc[0]

        c1, c2, c3, c4 = st.columns(4)
        total = float(snap["total_supply_ip"]) if snap["total_supply_ip"] is not None else 0.0
        circ = float(snap["circulating_ip"]) if snap["circulating_ip"] is not None else 0.0
        locked = float(snap["locked_ip"]) if snap["locked_ip"] is not None else 0.0
        circ_pct = (100.0 * circ / total) if total else 0.0

        c1.metric("Total supply (IP)", f"{total:,.0f}")
        c2.metric("Circulating (IP)", f"{circ:,.0f}")
        c3.metric("Locked (IP)", f"{locked:,.0f}")
        c4.metric("Circulating (%)", f"{circ_pct:,.2f}%")

        st.line_chart(sup_until.set_index("ts")[["circulating_ip", "locked_ip"]])

        st.caption("Cumulative unlocked by allocation (same horizon)")
        un = q("""
            select unlock_date::date as ts, category, amount_ip
            from unlock_schedule
            order by ts, category
        """)
        if not un.empty:
            un["ts"] = pd.to_datetime(un["ts"]).dt.date
            un = un[un["ts"] <= horizon_date]
            if not un.empty:
                pivot = (
                    un.pivot_table(index="ts", columns="category", values="amount_ip", aggfunc="sum")
                      .fillna(0.0)
                      .sort_index()
                )
                st.area_chart(pivot.cumsum())
            else:
                st.caption("No unlocks up to the selected horizon.")
        else:
            st.caption("`unlock_schedule` is empty; cannot build category stack.")
except Exception as e:
    st.error(f"Failed to load/plot supply/unlocks: {e}")

# =========================================================
# 2) Unlock Schedule (table + annotated timeline + category stacks)
# =========================================================
st.subheader("Unlock Schedule (Upcoming & Timeline)")

try:
    un_all = q("""
        select unlock_date::date as ts, category, basis, amount_ip
        from unlock_schedule
        order by ts, category
    """)
    if un_all.empty:
        st.info("`unlock_schedule` is empty.")
    else:
        # --- Upcoming table ---
        un_up = un_all[un_all["ts"] >= pd.Timestamp.today().date()].copy()
        un_up["amount_ip"] = un_up["amount_ip"].round(2)
        st.dataframe(un_up.rename(columns={"ts": "unlock_date"}), use_container_width=True)

        # ---------- Chart A: Cumulative unlocked (all categories) with annotations ----------
        daily = (
            un_all.groupby("ts", as_index=False)["amount_ip"].sum()
                  .sort_values("ts")
        )
        daily["cum_unlocked"] = daily["amount_ip"].cumsum()

        tge_date = daily["ts"].min()
        biggest = daily.loc[daily["amount_ip"].idxmax()]

        def first_linear(cat: str):
            df = un_all[(un_all["category"] == cat) &
                        (un_all["basis"].str.lower().str.contains("linear"))]
            return df["ts"].min() if not df.empty else None

        first_linear_cc = first_linear("core_contributors")
        first_linear_eb = first_linear("early_backers")

        # --- Detect Foundation cliff (prefer from data; fallback to 2026-02-13) ---
        foundation_cliff = None
        try:
            fnd = un_all[
                (un_all["category"] == "foundation") &
                (un_all["basis"].str.lower().isin(["cliff", "mixed"]))
            ].sort_values("ts")
            if not fnd.empty:
                foundation_cliff = fnd["ts"].iloc[0]
        except Exception:
            pass
        if foundation_cliff is None:
            foundation_cliff = pd.to_datetime("2026-02-13").date()  # fallback

        import altair as alt
        base = alt.Chart(daily).encode(x=alt.X("ts:T", title="Date"))
        area = base.mark_area(opacity=0.35).encode(y=alt.Y("cum_unlocked:Q", title="Cumulative Unlocked (IP)"))
        line = base.mark_line().encode(y="cum_unlocked:Q")

        def rule_label(xdate, text, dy=-6):
            if xdate is None:
                return alt.Chart(pd.DataFrame())  # empty layer
            df = pd.DataFrame({"ts": [xdate], "label": [text]})
            rule = alt.Chart(df).mark_rule(color="red").encode(x="ts:T")
            txt = alt.Chart(df).mark_text(align="left", dx=6, dy=dy, color="red").encode(x="ts:T", y=alt.value(0), text="label:N")
            return rule + txt

        layers = area + line
        # TGE
        layers = layers + rule_label(tge_date, f"TGE ({tge_date})", dy=-10)
        # Largest one-day unlock
        if pd.notnull(biggest["ts"]):
            layers = layers + rule_label(
                biggest["ts"],
                f"Largest 1-day: {int(round(biggest['amount_ip'])):,} IP",
                dy=10
            )
        # Linear starts
        if first_linear_cc:
            layers = layers + rule_label(first_linear_cc, f"Core Contributors linear start ({first_linear_cc})", dy=-22)
        if first_linear_eb:
            layers = layers + rule_label(first_linear_eb, f"Early Backers linear start ({first_linear_eb})", dy=22)
        # Foundation cliff (50M) marker
        layers = layers + rule_label(foundation_cliff, f"Foundation cliff (≈50M) ({foundation_cliff})", dy=-34)

        st.altair_chart(layers.properties(width="container", height=260), use_container_width=True)


        # ---------- Chart B: Category-stacked cumulative + vesting-start markers ----------
        # 1) daily per-category -> cumulative
        cat_daily = (
            un_all.groupby(["ts", "category"], as_index=False)["amount_ip"].sum()
                  .sort_values(["ts", "category"])
        )
        wide = cat_daily.pivot(index="ts", columns="category", values="amount_ip").fillna(0.0)
        wide_cum = wide.cumsum()
        cum_long = (wide_cum.reset_index()
                              .melt(id_vars="ts", var_name="category", value_name="cum_ip")
                              .sort_values(["ts", "category"]))

        stack = (
            alt.Chart(cum_long)
               .mark_area(opacity=0.6)
               .encode(
                   x=alt.X("ts:T", title="Date"),
                   y=alt.Y("cum_ip:Q", title="Cumulative Unlocked (IP)"),
                   color=alt.Color("category:N", title="Allocation")
               )
               .properties(width="container", height=280)
        )

        # 2) Vesting start per cohort = first linear day in that category
        vesting_starts = []
        for cat, grp in un_all.groupby("category", as_index=False):
            lin = grp[grp["basis"].str.lower().str.contains("linear")]
            if not lin.empty:
                vesting_starts.append({"category": cat, "ts": lin["ts"].min()})

        markers = alt.Chart(pd.DataFrame())
        labels_layer = alt.Chart(pd.DataFrame())
        if vesting_starts:
            vest_df = pd.DataFrame(vesting_starts)
            # join to get Y coordinate at that date
            vest_join = vest_df.merge(cum_long, on=["ts", "category"], how="left")
            # markers with tooltip (no overlap issue)
            markers = (
                alt.Chart(vest_join)
                   .mark_point(filled=True, size=80, opacity=0.9)
                   .encode(
                       x="ts:T",
                       y="cum_ip:Q",
                       color="category:N",
                       tooltip=[alt.Tooltip("category:N", title="Cohort"),
                                alt.Tooltip("ts:T", title="Vesting start")]
                   )
            )

            # add a few non-overlapping labels (limit to 6, stagger dy)
            vest_join = vest_join.sort_values("ts").copy()
            vest_join["label"] = vest_join["category"].str.replace("_", " ").str.title()
            vest_join["dy"] = (vest_join.groupby("ts").cumcount() * 12) - 8
            vest_limited = vest_join.head(6)

            label_layers = []
            for _, row in vest_limited.iterrows():
                label_layers.append(
                    alt.Chart(pd.DataFrame([row]))
                       .mark_text(align="left", dx=6, dy=int(row["dy"]), color="#444")
                       .encode(
                           x="ts:T",
                           y="cum_ip:Q",
                           text=alt.value(row["label"]),
                       )
                )
            if label_layers:
                labels_layer = alt.layer(*label_layers)

        st.altair_chart((stack + markers + labels_layer).resolve_scale(color="independent"), use_container_width=True)
        st.caption("Stacked cumulative unlocks by allocation. Dots mark each cohort's **linear vesting start** (hover for exact date). Labels are limited and staggered to avoid overlap.")

except Exception as e:
    st.error(f"Failed to render unlock timeline: {e}")

# =========================================================
# 3) Exchange net flows (ALL CEX) — daily vs cumulative (step)
# =========================================================
st.subheader("Exchange Net Flows (ALL CEX)")
try:
    ex = q("""
        select asof::date as asof, time_window, net_in_ip, unlock_proximity
        from exchange_flows
        where exchange = 'ALL'
        order by asof
    """)
    if ex.empty:
        st.info(
            "No exchange flow rows yet. Seed a couple of CEX addresses in `config/cex_addresses.txt`, "
            "then run: `python etl/labels_rules.py && python etl/flows.py`."
        )
    else:
        # --- choose window (prefer 1d>3d>7d>30d if available), but let user override
        windows_present = sorted(ex["time_window"].unique(), key=lambda w: ["1d","3d","7d","30d"].index(w) if w in ["1d","3d","7d","30d"] else 99)
        picked = st.radio("Window", windows_present, index=0, horizontal=True)

        exw = (
            ex.loc[ex["time_window"] == picked, ["asof", "net_in_ip"]]
              .dropna()
              .sort_values("asof")
              .reset_index(drop=True)
        )
        if exw.empty:
            st.info(f"No rows for window {picked}.")
        else:
            # Chart selector
            mode = st.radio("View", ["Daily net flow (bars)", "Cumulative (step)"], horizontal=True)

            import altair as alt
            exw_ren = exw.rename(columns={"asof": "date", "net_in_ip": "net_in_ip"})
            exw_ren["cum_in_ip"] = exw_ren["net_in_ip"].cumsum()

            if mode == "Daily net flow (bars)":
                chart = (
                    alt.Chart(exw_ren)
                       .mark_bar()
                       .encode(
                           x=alt.X("date:T", title="Date"),
                           y=alt.Y("net_in_ip:Q", title="Net flow to exchanges (IP/day)"),
                           tooltip=[
                               alt.Tooltip("date:T", title="Date"),
                               alt.Tooltip("net_in_ip:Q", title="Net flow (IP)", format=",.2f"),
                           ],
                       )
                       .properties(width="container", height=260)
                )
                st.altair_chart(chart, use_container_width=True)
                st.caption(f"Window: **{picked}**. Bars show *per-day* net flow. "
                           "Positive = net inflow to exchanges (potential sell pressure); negative = withdrawals.")
            else:
                chart = (
                    alt.Chart(exw_ren)
                       .mark_line(interpolate="step-after")
                       .encode(
                           x=alt.X("date:T", title="Date"),
                           y=alt.Y("cum_in_ip:Q", title="Cumulative net flow to exchanges (IP)"),
                           tooltip=[
                               alt.Tooltip("date:T", title="Date"),
                               alt.Tooltip("cum_in_ip:Q", title="Cumulative (IP)", format=",.2f"),
                           ],
                       )
                       .properties(width="container", height=260)
                )
                st.altair_chart(chart, use_container_width=True)
                st.caption(f"Window: **{picked}**. Step line shows *cumulative* net flow (sum of daily values). "
                           "A rising step means sustained net deposits to exchanges; flat/declining means neutral/withdrawals.")
except Exception as e:
    st.error(f"Exchange flows failed: {e}")

# =========================================================
# 4) Concentration: Top10/Top50 + separate Gini / HHI
# =========================================================
st.subheader("Holder Concentration")
try:
    conc = q("select ts, top10_share, top50_share, hhi, gini from concentration_timeseries order by ts;")
    if conc.empty:
        st.info("Run `python etl/balances_latest.py --days 7` then `python etl/concentration.py --all`.")
    else:
        # Ensure proper dtypes
        conc["ts"] = pd.to_datetime(conc["ts"])
        for col in ["top10_share", "top50_share", "hhi", "gini"]:
            conc[col] = pd.to_numeric(conc[col], errors="coerce")

        latest = conc.iloc[-1]

        # Metrics row: only Top10 / Top50 here
        m1, m2 = st.columns(2)
        m1.metric("Top 10 share", f"{float(latest['top10_share']):.2f}%")
        m2.metric("Top 50 share", f"{float(latest['top50_share']):.2f}%")

        # Chart 1: percentage shares
        st.line_chart(conc.set_index("ts")[["top10_share", "top50_share"]])

        # Two-column layout for Gini and HHI charts + local metrics
        c_left, c_right = st.columns(2)

        with c_left:
            st.markdown("**Gini (inequality of balances)**")
            if conc["gini"].notnull().any():
                st.line_chart(conc.set_index("ts")[["gini"]])
                gval = float(latest["gini"]) if pd.notnull(latest["gini"]) else None
                st.metric("Latest Gini (%)", f"{gval:.2f}%" if gval is not None else "—")
            else:
                st.info("Gini is empty. Run `python etl/concentration.py --all` to populate.")

        with c_right:
            st.markdown("**HHI (0–10,000 scale)**")
            st.line_chart(conc.set_index("ts")[["hhi"]])
            st.metric("Latest HHI", f"{float(latest['hhi']):.0f}")

except Exception as e:
    st.error(f"Concentration failed: {e}")

st.caption(
    "Note: Concentration metrics (Top10/Top50, HHI, Gini) are computed from a "
    "30-day backfill (Aug 2025). This means only addresses active during this "
    "period are included. Dormant large holders who received allocations at TGE "
    "(Feb 2025) but have not moved tokens recently are not fully captured. "
    "As a result, values are directionally correct (showing heavy concentration "
    "among a few addresses) but may understate total balances of early investors/foundation. "
    "Full history backfill from TGE would refine these metrics."
)

# =========================================================
# 5) Top holders (today) with labels & behavior flags
# =========================================================
st.subheader("Top Holders (with Labels & Flags)")
try:
    holders = q("""
        with d as (select max(asof) as asof from top_holders_snapshot),
        flags as (
          select address, string_agg(distinct flag, ', ' order by flag) as flags
          from holder_flags
          where asof = current_date
          group by 1
        )
        select th.rnk,
               '0x'||encode(th.address,'hex') as address,
               round(th.balance_ip,4) as balance_ip,
               coalesce(al.label,'') as label,
               coalesce(al.category,'unknown') as category,
               coalesce(al.confidence,'') as confidence,
               coalesce(flags.flags,'') as flags
        from top_holders_snapshot th
        left join address_labels al on al.address = th.address
        left join flags on flags.address = th.address
        where th.asof = (select asof from d)
        order by th.rnk
        limit 50;
    """)
    if holders.empty:
        st.info("Run `python etl/balances_latest.py` then `python etl/concentration.py` "
                "to populate top holders; run `python etl/flags.py` to add behavior flags.")
    else:
        st.dataframe(holders, use_container_width=True)
except Exception as e:
    st.error(f"Top holders failed: {e}")

# =========================================================
# 6) Manually Identified Exchange Clusters (from probes)
# =========================================================
st.subheader("Probe Findings — CEX HOT & Treasuries (All Labeled)")

try:
    probe = q("""
        with latest as (
          select max(sampled_at) ts from balances_latest
        ),
        bal as (
          select address, balance_ip
          from balances_latest
          where sampled_at = (select ts from latest)
        ),
        labs as (
          select address, label, category, coalesce(confidence,'') confidence
          from address_labels
          where category in ('cex','cex_cluster')
        ),
        enriched as (
          select
            case
              when category='cex'         then split_part(label,'_',3)
              when category='cex_cluster' then split_part(label,':',1)
              else '' end                                         as exchange,
            case
              when category='cex'         then 'HOT'
              when category='cex_cluster' then split_part(label,':',2)
              else '' end                                         as cluster,
            case
              when category='cex'         then 'HOT'
              when category='cex_cluster' then coalesce(nullif(split_part(label,':',3),''),'CLUSTER')
              else '' end                                         as role,
            '0x'||encode(l.address,'hex')                         as address,
            round(greatest(coalesce(b.balance_ip,0),0)::numeric, 2) as balance_ip,
            label, category, confidence
          from labs l
          left join bal b on b.address = l.address
        )
        select *
        from enriched
        order by
          exchange nulls last,
          cluster  nulls last,
          case when category='cex' then 0 else 1 end,
          balance_ip desc nulls last;
    """)
    if probe.empty:
        st.info("No manually seeded clusters found.\n\n• Keep only HOT wallets in `config/cex_addresses.txt`\n• Run: `python etl/labels_rules.py`\n• (Optional) Add treasuries/cluster edges with INSERTs into `address_labels`.")
    else:
        st.dataframe(probe, use_container_width=True)
except Exception as e:
    st.error(f"Exchange clusters failed: {e}")

st.caption(
    "Balances here reflect **net flows over the backfill window (August 2025)**, not full token history. "
    "Some exchange or cluster wallets may appear as `0` because their main activity happened "
    "before the backfill period. Negative balances were clamped to `0` for interpretability. "
    "Clusters and HOT wallets shown here were identified via manual **probe queries**."
)

# =========================================================
# 7) Post-unlock behavior (optional; needs realized_unlocks + CEX labels)
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
        st.info(
            "No unlock events fall inside the current 30-day backfill window, so post-unlock sell-through is "
            "**not applicable** for this dataset. The next major Story unlock (Foundation 5% cliff + vesting start) "
            "is scheduled for **Feb 13, 2026**, which is outside the data window shown here. "
            "See the Unlock Schedule charts above for upcoming events."
        )
        with st.expander("How to demo this section (optional)"):
            st.markdown(
                "- Add a mock row into `realized_unlocks` for a recent date and a known beneficiary, **or**\n"
                "- Backfill chain data that spans Feb 2026 (and ideally traces) so real unlocks can be attributed.\n\n"
                "Once `realized_unlocks` has data and a few CEX labels exist, this table will populate automatically."
            )
    else:
        st.dataframe(post, use_container_width=True)

except Exception as e:
    st.error(f"Failed to compute post-unlock behavior: {e}")

# =========================================================
# 7) Data health (quick debug)
# =========================================================
with st.expander("Data health (debug)"):
    try:
        def exists(table):
            return not q(f"select 1 from information_schema.tables where table_name='{table}';").empty

        counts = {
            "blocks": q("select count(*) c from blocks;")["c"].iloc[0] if exists("blocks") else 0,
            "transactions": q("select count(*) c from transactions;")["c"].iloc[0] if exists("transactions") else 0,
            "ip_transfers": q("select count(*) c from ip_transfers;")["c"].iloc[0] if exists("ip_transfers") else 0,
            "balances_latest": q("select count(*) c from balances_latest;")["c"].iloc[0] if exists("balances_latest") else 0,
            "top_holders_snapshot": q("select count(*) c from top_holders_snapshot;")["c"].iloc[0] if exists("top_holders_snapshot") else 0,
            "concentration_timeseries": q("select count(*) c from concentration_timeseries;")["c"].iloc[0] if exists("concentration_timeseries") else 0,
            "exchange_flows": q("select count(*) c from exchange_flows;")["c"].iloc[0] if exists("exchange_flows") else 0,
            "address_labels": q("select count(*) c from address_labels;")["c"].iloc[0] if exists("address_labels") else 0,
            "holder_flags": q("select count(*) c from holder_flags;")["c"].iloc[0] if exists("holder_flags") else 0,
            "unlock_schedule": q("select count(*) c from unlock_schedule;")["c"].iloc[0] if exists("unlock_schedule") else 0,
            "supply_timeseries": q("select count(*) c from supply_timeseries;")["c"].iloc[0] if exists("supply_timeseries") else 0,
            "realized_unlocks": q("select count(*) c from realized_unlocks;")["c"].iloc[0] if exists("realized_unlocks") else 0,
        }
        st.json(counts)
    except Exception as e:
        st.write(f"Health check error: {e}")
