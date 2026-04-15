WITH CTE AS (
    SELECT {{ select_all_columns(source('WEALTH', 'FUND')) }}
        , DATE_FROM_PARTS(LEFT(RIGHT("_file", 12), 4), LEFT(RIGHT("_file", 8), 2),LEFT(RIGHT("_file", 6), 2) ) AS FILE_BUSINESS_DATE
    FROM {{ source('WEALTH', 'FUND') }}
)

SELECT * FROM CTE 
QUALIFY RANK() OVER(PARTITION BY "_file" ORDER BY FILE_BUSINESS_DATE DESC) = 1