{#- Wilson score interval for a binomial proportion (wins / games).

    Unlike the plain p ± z*sqrt(p(1-p)/n), it stays inside [0, 1] and behaves sensibly for small
    samples: a champion that went 3-0 gets roughly [0.44, 1.00], not a "100% win rate".

        center = (p + z²/2n) / (1 + z²/n)
        margin = z * sqrt(p(1-p)/n + z²/4n²) / (1 + z²/n)
-#}
{% macro wilson_bound(successes, trials, side, z=var('wilson_z')) -%}
    {%- set sign = '-' if side == 'lower' else '+' -%}
    case when {{ trials }} = 0 then null else
    (
        ({{ successes }} / {{ trials }}) + power({{ z }}, 2) / (2 * {{ trials }})
        {{ sign }} {{ z }} * sqrt(
            ({{ successes }} / {{ trials }}) * (1 - {{ successes }} / {{ trials }}) / {{ trials }}
            + power({{ z }}, 2) / (4 * power({{ trials }}, 2))
        )
    ) / (1 + power({{ z }}, 2) / {{ trials }})
    end
{%- endmacro %}
