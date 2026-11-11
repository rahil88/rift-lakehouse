-- Champion win rate by patch and role, with sample size and a 95% Wilson interval.
-- Remakes are excluded. Rows with few games are kept on purpose: the interval width shows how
-- little they mean, and the consumer filters on `games` (the dashboard uses games >= 20).
with games as (
    select * from {{ ref('fct_participant_match') }}
    where not is_remake and role is not null
),

agg as (
    select
        patch,
        champion_id,
        role,
        count(*)                                  as games,
        sum(case when win then 1 else 0 end)      as wins
    from games
    group by patch, champion_id, role
)

select
    a.patch,
    a.champion_id,
    c.champion_name,
    a.role,
    a.games,
    a.wins,
    round(a.wins / a.games, 4)                                          as win_rate,
    round({{ wilson_bound('a.wins', 'a.games', 'lower') }}, 4)          as win_rate_ci_lower,
    round({{ wilson_bound('a.wins', 'a.games', 'upper') }}, 4)          as win_rate_ci_upper,
    a.games / sum(a.games) over (partition by a.patch, a.role)          as pick_share_in_role
from agg a
left join {{ ref('dim_champion') }} c on a.champion_id = c.champion_id
