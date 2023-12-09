WITH cancers_in_year_of_the_rabbit AS (
    SELECT customerid
    , ST_Point(lat, long) as home
    FROM {{ ref('stg_customers') }}
    WHERE year(birth_dt) IN (1927, 1939, 1951, 1963, 1975, 1987, 1999, 2011, 2023)
    AND
    (
        (monthname(birth_dt) = 'June' AND day(birth_dt) BETWEEN 22 AND 30)
        OR (monthname(birth_dt) = 'July' AND day(birth_dt) BETWEEN 1 AND 22)
    )
),
contractor_lat_long AS (
    SELECT customerid
    , ST_Point(lat, long) as home
    FROM {{ ref('stg_customers') }}
    WHERE customerid IN (SELECT customerid FROM {{ ref('two') }})
),
closest_cancer_contractor AS (
    SELECT c.customerid
    , ST_Distance(cl.home, c.home) as distance
    FROM cancers_in_year_of_the_rabbit c
    CROSS JOIN contractor_lat_long cl
    ORDER BY distance
    LIMIT 1
)
SELECT c.customerid, c.name, c.phone
FROM {{ ref('stg_customers') }} c
INNER JOIN closest_cancer_contractor cc ON c.customerid = cc.customerid
