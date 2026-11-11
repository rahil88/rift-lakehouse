-- Every completed match has exactly 5 winners, all on the same team.
select match_id, count(*) as winners, count(distinct team_id) as winning_teams
from {{ ref('fct_participant_match') }}
where win and not is_remake
group by match_id
having count(*) != 5 or count(distinct team_id) != 1
