WITH bakery_orders AS (
    SELECT orderid
    FROM {{ ref('stg_orders_items')}}
    WHERE sku LIKE 'BKY%'
),
early_bakery_orders AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
    WHERE orderid IN (SELECT orderid FROM bakery_orders)
    AND hour(shipped) < 5
),
top_early_bakery_customer AS (
    SELECT customerid
    , COUNT(1) cnt
    FROM early_bakery_orders
    GROUP BY customerid
    ORDER BY cnt DESC
    LIMIT 1
)
SELECT c.customerid, c.name, c.phone
FROM {{ ref('stg_customers') }} c
INNER JOIN top_early_bakery_customer t ON c.customerid = t.customerid
