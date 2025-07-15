{% macro limit_data_in_dev(date_column, dev_days = 3) %}
{% if target.name == 'default' %}
WHERE {{ date_column }} >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ dev_days }} DAY)
{% endif %}
{% endmacro %}