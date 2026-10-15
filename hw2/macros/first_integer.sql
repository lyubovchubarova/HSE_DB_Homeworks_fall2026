{% macro first_integer(text_expression) -%}
nullif(substring({{ text_expression }} from '[0-9]+'), '')::integer
{%- endmacro %}
