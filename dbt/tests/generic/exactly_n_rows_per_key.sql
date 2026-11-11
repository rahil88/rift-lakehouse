{#- Fails for every key value that does not have exactly n rows.
    Used for "every match has exactly 10 participants": catches a broken explode, a bad join,
    or a dedup bug that the row-level tests can't see. -#}
{% test exactly_n_rows_per_key(model, column_name, n) %}
select {{ column_name }}, count(*) as row_count
from {{ model }}
group by {{ column_name }}
having count(*) != {{ n }}
{% endtest %}
