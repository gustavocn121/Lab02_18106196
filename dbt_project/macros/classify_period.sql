{% doc classify_period %}
Macro que classifica o trimestre.
{% enddoc %}

{% macro classify_period(trimestre_col) %}
CASE
    WHEN {{ trimestre_col }} = 1 THEN 'Q1 - Verão/Carnaval'
    WHEN {{ trimestre_col }} = 2 THEN 'Q2 - Outono'
    WHEN {{ trimestre_col }} = 3 THEN 'Q3 - Inverno/Férias'
    WHEN {{ trimestre_col }} = 4 THEN 'Q4 - Primavera/Natal'
    ELSE NULL
END
{% endmacro %}
