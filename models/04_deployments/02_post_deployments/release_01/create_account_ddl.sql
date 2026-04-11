{% set db = env_var('DBT_ENV_PROVIDER_SYSTEMS') %}

{{ config(
    materialized='view',
    database=db,
    schema='WEALTH',
    tags=['deployment']
) }}

{% set sql %}
CREATE OR REPLACE TABLE {{ db }}.WEALTH.TEST_TABLE
(   
    ID VARCHAR(100),
    NAME VARCHAR(200)
);
{% endset %}

{% do run_query(sql) %}

select 1 as dummy