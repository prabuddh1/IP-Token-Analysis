-- ========= Part 1 Views =========
-- These views assume you already have:
--   blocks, ip_transfers, unlock_schedule, supply_timeseries, address_labels

-- 1) First-seen timestamp per address (used for “fresh wallet” heuristics later)
create materialized view if not exists mv_address_first_seen as
select a.address, min(b.timestamp) as first_seen_ts
from (
  select from_address as address, block_number from ip_transfers
  union
  select to_address   as address, block_number from ip_transfers
) a
join blocks b on b.number = a.block_number
group by 1;

-- 2) Net flow per address per day (wei)
create materialized view if not exists mv_addr_netflow_daily as
with x as (
  select date_trunc('day', b.timestamp)::date as ts, it.from_address as address, -(it.value_wei) as delta_wei
  from ip_transfers it join blocks b on b.number = it.block_number
  union all
  select date_trunc('day', b.timestamp)::date as ts, it.to_address   as address, +(it.value_wei) as delta_wei
  from ip_transfers it join blocks b on b.number = it.block_number
)
select ts, address, sum(delta_wei) as net_wei
from x
group by 1,2
order by 1,2;

-- 3) Daily balance per address = cumulative sum of net flows (wei)
create materialized view if not exists mv_addr_balance_daily as
select ts, address,
       sum(net_wei) over (partition by address order by ts rows unbounded preceding) as balance_wei
from mv_addr_netflow_daily
order by 1,2;

-- 4) Non-circulating address set (edit categories/labels to taste)
create materialized view if not exists mv_non_circ_addrs as
select address
from address_labels
where coalesce(confidence,'medium') <> 'low'
  and (
    category in ('treasury','foundation','investor','team','vesting','escrow','contract')
    or label ilike '%multisig%'
    or label ilike '%vesting%'
    or label ilike '%lockup%'
    or label ilike '%escrow%'
  );

-- 5) Non-circulating balance per day (IP)
create materialized view if not exists mv_non_circ_balance_daily as
select b.ts, sum(b.balance_wei)/1e18::numeric as non_circ_ip
from mv_addr_balance_daily b
join mv_non_circ_addrs n on n.address = b.address
group by 1
order by 1;

-- 6) Exchange balances per exchange label per day (IP)
create materialized view if not exists mv_exchange_balance_daily as
select b.ts, l.label as exchange, sum(b.balance_wei)/1e18::numeric as balance_ip
from mv_addr_balance_daily b
join address_labels l on l.address = b.address
where l.category = 'cex'
group by 1,2
order by 1,2;

-- 7) Percent of circulating held on exchanges
create materialized view if not exists mv_exchange_share_daily as
with s as (select ts, circulating_ip from supply_timeseries),
e as (select ts, sum(balance_ip) as cex_ip from mv_exchange_balance_daily group by 1)
select s.ts,
       s.circulating_ip,
       coalesce(e.cex_ip,0) as cex_ip,
       case when s.circulating_ip > 0
            then round(100.0*coalesce(e.cex_ip,0)/s.circulating_ip, 4)
            else 0 end as cex_pct_of_circ
from s
left join e on e.ts = s.ts
order by s.ts;

-- 8) Adjusted circulating (subtracting non-circulating balances)
create materialized view if not exists mv_circulating_adjusted as
select s.ts,
       s.total_supply_ip,
       s.circulating_ip,
       coalesce(n.non_circ_ip,0) as non_circ_ip,
       greatest(s.circulating_ip - coalesce(n.non_circ_ip,0), 0) as circulating_adj_ip,
       greatest(s.total_supply_ip - (s.circulating_ip - coalesce(n.non_circ_ip,0)), 0) as locked_adj_ip
from supply_timeseries s
left join mv_non_circ_balance_daily n on n.ts = s.ts
order by s.ts;
