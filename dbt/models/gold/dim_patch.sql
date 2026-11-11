select
    patch,
    {{ patch_sort_key('patch') }}           as patch_sort_key,
    cast(split(patch, '\\.')[0] as int)    as major,
    cast(split(patch, '\\.')[1] as int)    as minor,
    min(game_created_at)                   as first_game_at,
    max(game_created_at)                   as last_game_at,
    count(*)                               as matches
from {{ ref('stg_matches') }}
group by patch
