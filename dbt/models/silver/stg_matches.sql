{{ config(unique_key='match_id') }}

with source as (
    select * from {{ source('bronze', 'matches') }}
    {{ incremental_since() }}
),

parsed as (
    select
        payload:metadata.matchId::string                           as match_id,
        payload:info.gameId::bigint                                as game_id,
        payload:info.platformId::string                            as platform_id,
        payload:info.queueId::int                                  as queue_id,
        payload:info.mapId::int                                    as map_id,
        payload:info.gameVersion::string                           as game_version,
        {{ parse_patch("payload:info.gameVersion::string") }}      as patch,
        timestamp_millis(payload:info.gameCreation::bigint)        as game_created_at,
        timestamp_millis(payload:info.gameStartTimestamp::bigint)  as game_started_at,
        timestamp_millis(payload:info.gameEndTimestamp::bigint)    as game_ended_at,
        payload:info.gameDuration::int                             as game_duration_s,
        payload:info.endOfGameResult::string                       as end_of_game_result,
        case
            when payload:info.teams[0].win::boolean then payload:info.teams[0].teamId::int
            when payload:info.teams[1].win::boolean then payload:info.teams[1].teamId::int
        end                                                        as winning_team_id,
        event_id,
        _schema_version,
        _source_file,
        _ingested_at
    from source
)

-- the same match can arrive twice (at-least-once delivery): keep the latest copy
select *
from parsed
qualify row_number() over (partition by match_id order by _ingested_at desc, _source_file desc) = 1
