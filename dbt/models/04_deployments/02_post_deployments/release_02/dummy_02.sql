-- set database into variable


-- set code cofigurations
{{ config(
    materialized='view',
    schema='WEALTH',
    tags=['deployment']
) }}


{% set sql %}
-- code starts here-----------------------------------------------------------

CREATE OR REPLACE TABLE MONITOR_DEV.WEALTH.TEST_TABLE
(   
    ID VARCHAR(100),
    NAME VARCHAR(200)
);

-- code ends here-------------------------------------------------------------
{% endset %}

{% do run_query(sql) %}

select 1 as dummy