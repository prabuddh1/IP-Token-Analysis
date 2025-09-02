create index if not exists idx_tx_block on transactions(block_number);
create index if not exists idx_ip_from on ip_transfers(from_address);
create index if not exists idx_ip_to on ip_transfers(to_address);
create index if not exists idx_ip_block on ip_transfers(block_number);