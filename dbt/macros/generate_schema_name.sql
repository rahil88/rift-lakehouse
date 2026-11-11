{#- Use the configured schema name as-is (rift_silver, rift_gold) instead of dbt's default
    "<target_schema>_<custom_schema>", so tables land where the quality checks and dashboard
    expect them. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
