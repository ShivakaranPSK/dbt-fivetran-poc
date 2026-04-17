{{ config(materialized='table') }}

WITH base AS (
    SELECT
        ACCOUNT_NO,
        CUSTOMER_NUMBER,
        AC_NAME AS ACCOUNT_NAME,
        AC_TYPE AS ACCOUNT_TYPE,
        INVEST_TYPE,
        BRANCH_CD,
        PRIMARY_FLAG,
        AC_OPEN_DATE,
        CREATED_AT AS START_DATE
    FROM {{ ref('WEALTH_CUSTOMER_INVESTMENT_ACCOUNT') }}
),

scd AS (
    SELECT
        *,
        LEAD(START_DATE) OVER (
            PARTITION BY ACCOUNT_NO
            ORDER BY START_DATE
        ) AS NEXT_START_DATE
    FROM base
),

customer_dim as (select * from  {{ref('dim_customer')}} )

SELECT
    ROW_NUMBER() OVER (ORDER BY a.ACCOUNT_NO,  a.START_DATE) AS ACCOUNT_SK,

    a.ACCOUNT_NO,
    c.CUSTOMER_SK AS CUSTOMER_SK, 

    a.ACCOUNT_NAME,
    a.ACCOUNT_TYPE,
     a.INVEST_TYPE AS INVESTMENT_TYPE,
     a.BRANCH_CD AS BRANCH_CODE,

    CASE WHEN  a.PRIMARY_FLAG = '1' THEN TRUE ELSE FALSE END AS IS_PRIMARY_FLAG,

     a.AC_OPEN_DATE AS ACCOUNT_OPEN_DATE,

     a.START_DATE ,
    DATEADD(SECOND, -1,  a.NEXT_START_DATE) AS END_DATE,

    CASE 
        WHEN  a.NEXT_START_DATE IS NULL THEN TRUE 
        ELSE FALSE 
    END AS IS_CURRENT,

    CURRENT_TIMESTAMP() AS DW_LOAD_TIMESTAMP

FROM scd a left join customer_dim c ON a.CUSTOMER_NUMBER=c.CIF_NO and a.START_DATE BETWEEN c.START_DATE and coalesce(c.END_DATE,'9999-12-31')