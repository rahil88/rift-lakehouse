{#- SCD Type 2 rank history. league-v4 only returns *current* rank, so history exists only
    from the first snapshot onward and cannot be backfilled. Each run compares the latest
    ladder entry per player to the open record: a change in tier, division or LP closes the old
    row (dbt_valid_to) and opens a new one. Players who leave the ladder are closed out
    (hard_deletes='invalidate'). Re-running with unchanged data creates no new versions, so the
    snapshot is idempotent. -#}
{% snapshot snap_player_rank %}
{{
    config(
        unique_key=['puuid', 'queue'],
        strategy='check',
        check_cols=['tier', 'division', 'league_points'],
        hard_deletes='invalidate',
    )
}}

select
    puuid,
    queue,
    platform_id,
    tier,
    tier_order,
    division,
    league_points,
    wins,
    losses,
    snapshot_at as observed_at
from {{ ref('stg_player_ranks') }}
qualify row_number() over (partition by puuid, queue order by snapshot_at desc) = 1

{% endsnapshot %}
