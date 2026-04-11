-- macros/log_model_runs.sql

{% macro log_model_runs(results) %}

    {% for res in results %}

        {% set node = res.node %}

        {# ❌ Skip deployment models #}
        {% if 'deployment' in node.tags 
              or 'deployments' in node.original_file_path %}

            {{ log("Skipping deployment model: " ~ node.name, info=True) }}

        {% else %}

            {% set insert_sql %}
                insert into {{ env_var('DBT_ENV_MONITOR') }}.MONITOR.DBT_MODEL_RUN_LOG
                (
                    model_name,
                    status,
                    execution_time,
                    executed_at,
                    database_name,
                    schema_name
                )
                values
                (
                    '{{ node.name }}',
                    '{{ res.status }}',
                    {{ res.execution_time }},
                    current_timestamp,
                    '{{ node.database }}',
                    '{{ node.schema }}'
                )
            {% endset %}

            {{ log("Logging model: " ~ node.name, info=True) }}

            {% do run_query(insert_sql) %}

        {% endif %}

    {% endfor %}

{% endmacro %}