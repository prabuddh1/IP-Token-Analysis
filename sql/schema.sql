-- =========================
-- Core chain + ETL support
-- =========================
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
  success boolean not null,
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

-- Backfill checkpoint
create table if not exists sync_state (
  id text primary key,
  last_block bigint,
  updated_at timestamptz default now()
);

create index if not exists idx_ip_block on ip_transfers(block_number);
create index if not exists idx_ip_from  on ip_transfers(from_address);
create index if not exists idx_ip_to    on ip_transfers(to_address);

-- =========================
-- Address labels & holders
-- =========================
create table if not exists address_labels (
  address bytea primary key,
  label text,                      -- freeform like 'CEX:Hot', 'Treasury/Foundation'
  category text,                   -- enum-like: 'cex','treasury','investor','team','ecosystem','mm','contract','unknown'
  confidence text,                 -- 'high'|'medium'|'low'
  rationale text,
  source text,                     -- 'seed','heuristic','manual','external'
  updated_at timestamptz default now()
);

create table if not exists balances_latest (
  sampled_at timestamptz not null,
  block_number bigint not null,
  address bytea not null,
  balance_ip numeric(38,6) not null,
  primary key (sampled_at, address)
);

create table if not exists top_holders_snapshot (
  asof date not null,
  rnk int not null,
  address bytea not null,
  balance_ip numeric(38,6) not null,
  primary key (asof, rnk)
);

create table if not exists concentration_timeseries (
  ts date primary key,
  top10_share numeric(10,4),
  top50_share numeric(10,4),
  hhi numeric(10,4),
  spike boolean default false
);

create table if not exists holder_flags (
  asof date not null,
  address bytea not null,
  flag text not null,              -- NEVER_SOLD | CONSISTENT_SELLER | ACCUMULATOR | REDISTRIBUTOR
  time_window text not null,       -- '30d' | '90d' | 'post-unlock:3d'
  confidence text not null,
  rationale text,
  primary key (asof, address, flag, time_window)
);

-- =========================
-- Unlocks & supply
-- =========================
create table if not exists unlock_schedule (
  unlock_date date not null,
  category text not null,          -- 'early_backers','contributors','foundation','ecosystem','incentives'
  amount_ip numeric(38,6) not null,
  basis text not null,             -- 'cliff','linear','tge'
  note text,
  primary key (unlock_date, category)
);

create table if not exists realized_unlocks (
  unlock_date date not null,
  category text not null,
  tx_hash bytea not null,
  src_address bytea,
  dst_address bytea,
  amount_wei numeric(78,0) not null,
  inferred boolean default true,
  primary key (unlock_date, category, tx_hash)
);

create table if not exists supply_timeseries (
  ts date primary key,
  total_supply_ip numeric(38,6) not null,
  circulating_ip numeric(38,6) not null,
  locked_ip numeric(38,6) not null
);

-- =========================
-- Exchange balances & flows
-- =========================
create table if not exists exchange_balances (
  ts date not null,
  exchange text not null,
  balance_ip numeric(38,6) not null,
  primary key (ts, exchange)
);

create table if not exists exchange_flows (
  time_window text not null,       -- '1d','3d','7d','30d'   (avoid reserved keyword 'window')
  asof date not null,
  exchange text not null,
  net_in_ip numeric(38,6) not null,
  unlock_proximity text not null,  -- 'pre','post','none'
  primary key (time_window, asof, exchange)
);
