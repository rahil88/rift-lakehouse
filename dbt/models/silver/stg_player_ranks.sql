{{ config(unique_key=['puuid', 'queue', 'snapshot_at']) }}

with source as (
    select * from {{ source('bronze', 'player_ranks') }}
    {{ incremental_since() }}
)

select
    payload:puuid::string                          as puuid,
    payload:queue::string                          as queue,
    payload:platformId::string                     as platform_id,
    payload:tier::string                           as tier,
    payload:rank::string                           as division,
    case payload:tier::string
        when 'CHALLENGER' then 0 when 'GRANDMASTER' then 1 when 'MASTER' then 2
        when 'DIAMOND' then 3 when 'EMERALD' then 4 when 'PLATINUM' then 5
        when 'GOLD' then 6 when 'SILVER' then 7 when 'BRONZE' then 8 when 'IRON' then 9
    end                                            as tier_order,
    payload:leaguePoints::int                      as league_points,
    payload:wins::int                              as wins,
    payload:losses::int                            as losses,
    payload:hotStreak::boolean                     as hot_streak,
    payload:veteran::boolean                       as veteran,
    payload:freshBlood::boolean                    as fresh_blood,
    payload:inactive::boolean                      as inactive,
    payload:snapshotAt::timestamp                  as snapshot_at,
    event_id,
    _schema_version,
    _ingested_at
from source
qualify row_number() over (
    partition by payload:puuid::string, payload:queue::string, payload:snapshotAt::timestamp
    order by _ingested_at desc
) = 1
