{% macro log_model_runs(results) %}

    {% for res in results %}

        {% set node = res.node %}

        {# ❌ Skip deployment models #}
        {% if 'deployment' in node.tags 
              or 'deployments' in node.original_file_path %}

            {{ log("Skipping deployment model: " ~ node.name, info=True) }}

        {% else %}

            {# Capture error safely #}
            {% set error_msg = res.message if res.message is not none else '' %}
            {% set env = env_var('DBT_ENVIRONMENT', 'unknown') %}

            {% set insert_sql %}
                insert into {{ env_var('DBT_ENV_MONITOR') }}.MONITOR.DBT_MODEL_RUN_LOG
                (
                    model_name,
                    status,
                    execution_time,
                    executed_at,
                    database_name,
                    schema_name,
                    run_id,
                    environment,
                    error_message
                )
                values
                (
                    '{{ node.name }}',
                    '{{ res.status }}',
                    {{ res.execution_time if res.execution_time is not none else 0 }},
                    current_timestamp,
                    '{{ node.database }}',
                    '{{ node.schema }}',
                    '{{ invocation_id }}',
                    '{{ env }}',
                    $$ {{ error_msg | replace("'", "''") }} $$
                )
            {% endset %}

            {{ log("Logging model: " ~ node.name ~ " | Status: " ~ res.status, info=True) }}

            {% do run_query(insert_sql) %}

        {% endif %}

    {% endfor %}

{% endmacro %}