-- Sanity check on the Wilson macro: 0 <= lower <= win_rate <= upper <= 1 for every row.
select *
from {{ ref('mart_champion_winrate_by_patch_role') }}
where not (
    win_rate_ci_lower >= 0
    and win_rate_ci_lower <= win_rate + 1e-9
    and win_rate <= win_rate_ci_upper + 1e-9
    and win_rate_ci_upper <= 1
)
