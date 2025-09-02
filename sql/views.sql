-- Top holders from sampled balances
create materialized view if not exists v_top_holders as
select address, balance_ip, rank() over (order by balance_ip desc) as rnk
from (
select address, sum(delta_ip) over (partition by address order by ts rows between unbounded preceding and current row) as balance_ip
from balance_deltas -- populated by balances_sampler.py
) t
where rnk <= 200;