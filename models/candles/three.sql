WITH cancers_in_year_of_the_rabbit AS (
    SELECT customerid
    , ST_Point(lat, long) as home
    FROM {{ ref('stg_customers') }}
    -- year of the goat check
    WHERE year(birthdate) IN (2027, 2015, 2003, 1991, 1979, 1967, 1955, 1943, 1931)
    AND
    (
        -- Cancer Zodiac check
        (monthname(birthdate) = 'September' AND day(birthdate) BETWEEN 23 AND 31)
        OR (monthname(birthdate) = 'October' AND day(birthdate) BETWEEN 1 AND 22)
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
