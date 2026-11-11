-- Grain: one row per player per match. Rebuilt as a table each run: deterministic and
-- trivially idempotent at this volume (~10 rows per match). Switch to incremental MERGE on
-- (match_id, participant_id) when the table gets large.
with participants as (
    select * from {{ ref('stg_participants') }}
),

matches as (
    select * from {{ ref('stg_matches') }}
)

select
    p.match_id,
    p.participant_id,
    p.puuid,
    p.champion_id,
    p.team_id,
    p.team_position                                               as role,
    p.win,
    p.kills,
    p.deaths,
    p.assists,
    round((p.kills + p.assists) / greatest(p.deaths, 1), 2)       as kda,
    p.creep_score,
    round(p.creep_score / (m.game_duration_s / 60.0), 2)          as cs_per_min,
    p.gold_earned,
    round(p.gold_earned / (m.game_duration_s / 60.0), 1)          as gold_per_min,
    p.damage_to_champions,
    p.vision_score,
    p.champ_level,
    m.patch,
    m.queue_id,
    m.game_duration_s,
    m.game_created_at,
    to_date(m.game_created_at)                                    as game_date,
    (m.game_duration_s < {{ var('remake_max_seconds') }}
        or p.ended_in_early_surrender)                            as is_remake
from participants p
inner join matches m on p.match_id = m.match_id
