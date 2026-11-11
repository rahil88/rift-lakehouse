{{ config(unique_key=['match_id', 'participant_id']) }}

{%- set participant_schema -%}
array<struct<
    participantId:int, puuid:string, championId:int, championName:string, teamId:int,
    teamPosition:string, individualPosition:string, win:boolean,
    kills:int, deaths:int, assists:int, champLevel:int, goldEarned:int,
    totalMinionsKilled:int, neutralMinionsKilled:int, totalDamageDealtToChampions:int,
    visionScore:double, gameEndedInEarlySurrender:boolean
>>
{%- endset %}

with source as (
    select * from {{ source('bronze', 'matches') }}
    {{ incremental_since() }}
),

latest as (
    select
        payload:metadata.matchId::string as match_id,
        payload,
        _ingested_at
    from source
    qualify row_number() over (
        partition by payload:metadata.matchId::string order by _ingested_at desc, _source_file desc
    ) = 1
),

exploded as (
    select
        match_id,
        _ingested_at,
        explode(from_json(payload:info.participants, '{{ participant_schema | replace("\n", " ") }}')) as p
    from latest
)

-- Riot IDs (player names) are deliberately not modeled: puuid is the stable, non-PII key.
select
    match_id,
    p.participantId                                   as participant_id,
    p.puuid                                           as puuid,
    p.championId                                      as champion_id,
    p.championName                                    as champion_name,
    p.teamId                                          as team_id,
    nullif(p.teamPosition, '')                        as team_position,
    p.win                                             as win,
    p.kills                                           as kills,
    p.deaths                                          as deaths,
    p.assists                                         as assists,
    p.champLevel                                      as champ_level,
    p.goldEarned                                      as gold_earned,
    p.totalMinionsKilled + p.neutralMinionsKilled     as creep_score,
    p.totalDamageDealtToChampions                     as damage_to_champions,
    p.visionScore                                     as vision_score,
    coalesce(p.gameEndedInEarlySurrender, false)      as ended_in_early_surrender,
    _ingested_at
from exploded
