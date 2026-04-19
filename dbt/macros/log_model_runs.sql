{% macro log_model_runs(results) %}

    {# Safe environment detection #}
    {% set env = target.name if target is defined and target.name is defined else 'unknown' %}

    {# Optional: dynamic monitor DB #}
    {% set monitor_db = 'MONITOR' if env == 'PROD' else 'MONITOR_' ~ env %}

    {{ log("========== DBT RUN LOGGING ==========", info=True) }}
    {{ log("Environment: " ~ env, info=True) }}
    {{ log("Monitor DB: " ~ monitor_db, info=True) }}

    {% for res in results %}

        {% set node = res.node %}

        {# Skip deployment models #}
        {% if 'deployment' in node.tags 
              or 'deployments' in node.original_file_path %}

            {{ log("Skipping deployment model: " ~ node.name, info=True) }}

        {% else %}

            {# Safe error capture #}
            {% set error_msg = res.message if res.message is not none else '' %}

            {% set insert_sql %}
                insert into {{ monitor_db }}.MONITOR.DBT_MODEL_RUN_LOG
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

            {{ log("Logging model: " ~ node.name ~ 
                   " | Status: " ~ res.status ~ 
                   " | Env: " ~ env, info=True) }}

            {% do run_query(insert_sql) %}

        {% endif %}

    {% endfor %}

    {{ log("========== END DBT RUN LOGGING ==========", info=True) }}

{% endmacro %}