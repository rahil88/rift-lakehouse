{#- "16.22.834.8910" -> "16.22": the patch is the first two parts of the game version. -#}
{% macro parse_patch(game_version) -%}
    regexp_extract({{ game_version }}, '^([0-9]+\\.[0-9]+)', 1)
{%- endmacro %}

{#- Sortable integer for a patch: "16.22" -> 1622, "16.3" -> 1603. -#}
{% macro patch_sort_key(patch) -%}
    cast(split({{ patch }}, '\\.')[0] as int) * 100 + cast(split({{ patch }}, '\\.')[1] as int)
{%- endmacro %}
