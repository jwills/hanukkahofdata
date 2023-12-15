SELECT 1 as q, q1.phone FROM {{ ref('one') }} q1
UNION ALL
SELECT 2 as q, q2.phone FROM {{ ref('two') }} q2
UNION ALL
SELECT 3 as q, q3.phone FROM {{ ref('three') }} q3
UNION ALL
SELECT 4 as q, q4.phone FROM {{ ref('four') }} q4
UNION ALL
SELECT 5 as q, q5.phone FROM {{ ref('five') }} q5
UNION ALL
SELECT 6 as q, q6.phone FROM {{ ref('six') }} q6
UNION ALL
SELECT 7 as q, q7.phone FROM {{ ref('seven') }} q7
UNION ALL
SELECT 8 as q, q8.phone FROM {{ ref('eight') }} q8
