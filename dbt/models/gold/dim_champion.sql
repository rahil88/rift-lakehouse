select
    champion_id,
    champion_name,
    ddragon_id,
    title,
    tags,
    element_at(tags, 1) as primary_class,
    ddragon_version
from {{ ref('stg_champions') }}
