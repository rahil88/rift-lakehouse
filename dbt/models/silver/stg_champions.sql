{{ config(materialized='table') }}

-- Latest Data Dragon version wins: names/tags are taken from the newest patch we have.
select
    champion_key                                  as champion_id,
    champion_id                                   as ddragon_id,
    name                                          as champion_name,
    title,
    tags,
    version                                       as ddragon_version,
    _ingested_at
from {{ source('bronze', 'ddragon_champions') }}
qualify row_number() over (
    partition by champion_key
    order by
        cast(split(version, '\\.')[0] as int) desc,
        cast(split(version, '\\.')[1] as int) desc,
        cast(split(version, '\\.')[2] as int) desc
) = 1
