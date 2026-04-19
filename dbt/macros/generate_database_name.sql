{% macro generate_database_name(custom_database_name, node) -%}

    {# SAFE fallback for parsing phase #}
    {%- set env = target.name if target is defined and target.name is defined else 'dev' -%}

    {%- set env = env | lower -%}

    {%- if custom_database_name is none or custom_database_name == '' -%}
        {{- (target.database if target is defined else 'DEFAULT_DB') -}}
    {%- elif env == 'prod' -%}
        {{- custom_database_name | upper -}}
    {%- else -%}
        {{- (custom_database_name ~ '_' ~ env) | upper -}}
    {%- endif -%}

{%- endmacro %}