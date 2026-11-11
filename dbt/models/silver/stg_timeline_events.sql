{{ config(unique_key=['match_id', 'frame_index', 'event_index']) }}

with source as (
    select * from {{ source('bronze', 'timeline_events') }}
    {{ incremental_since() }}
)

select
    payload:matchId::string                            as match_id,
    payload:frameIndex::int                            as frame_index,
    payload:eventIndex::int                            as event_index,
    payload:frameTimestamp::bigint                     as frame_timestamp_ms,
    payload:event.type::string                         as event_type,
    payload:event.timestamp::bigint                    as event_timestamp_ms,
    payload:event.participantId::int                   as participant_id,
    payload:event.killerId::int                        as killer_id,
    payload:event.victimId::int                        as victim_id,
    from_json(payload:event.assistingParticipantIds, 'array<int>') as assisting_participant_ids,
    payload:event.teamId::int                          as team_id,
    payload:event.killerTeamId::int                    as killer_team_id,
    payload:event.position.x::int                      as position_x,
    payload:event.position.y::int                      as position_y,
    payload:event.monsterType::string                  as monster_type,
    payload:event.monsterSubType::string               as monster_sub_type,
    payload:event.buildingType::string                 as building_type,
    payload:event.laneType::string                     as lane_type,
    payload:event.towerType::string                    as tower_type,
    payload:event.itemId::int                          as item_id,
    payload:event.wardType::string                     as ward_type,
    payload:event.bounty::int                          as bounty,
    payload:event as event_json, -- everything not modeled above, still queryable
    event_id,
    _schema_version,
    _ingested_at
from source
qualify row_number() over (
    partition by payload:matchId::string, payload:frameIndex::int, payload:eventIndex::int
    order by _ingested_at desc, _source_file desc
) = 1
