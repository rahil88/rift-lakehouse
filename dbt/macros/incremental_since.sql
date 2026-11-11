{#- Incremental filter for silver: only bronze rows loaded since the last run.
    Uses >= on purpose: it re-reads the last batch, and the MERGE on the natural key makes that a
    no-op, which is safer than risking a skipped batch at a timestamp boundary. -#}
{% macro incremental_since(column='_ingested_at') -%}
    {%- if is_incremental() -%}
    where {{ column }} >= (select coalesce(max({{ column }}), timestamp'1900-01-01') from {{ this }})
    {%- endif -%}
{%- endmacro %}
