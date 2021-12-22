{% macro from_binary(s) %}


(
	SELECT SUM(CAST(c AS INT64) << (LENGTH({{s}}) - 1 - bit))
FROM UNNEST(SPLIT({{s}}, '')) AS c WITH OFFSET bit)

{% endmacro %}
