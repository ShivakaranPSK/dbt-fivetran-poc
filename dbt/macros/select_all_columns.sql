{% macro select_all_columns(relation) %}

    {% set cols = adapter.get_columns_in_relation(relation) %}

    {% set col_names = [] %}
    {% for col in cols %}
        {% if col.name != col.name.upper() %}
            {% do col_names.append(adapter.quote(col.name)) %}
        {% else %}
            {% do col_names.append(col.name) %}
        {% endif %}
    {% endfor %}

    {{ return(col_names | join(', ')) }}

{% endmacro %}