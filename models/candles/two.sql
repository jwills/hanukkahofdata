WITH jps AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
    WHERE split_part(name, ' ', 1) LIKE 'D%'
    AND split_part(name, ' ', 2) LIKE 'S%'
),
coffee_and_bagels AS (
    SELECT *
    FROM {{ ref('stg_products') }}
    WHERE "desc" LIKE '%offee%'
    OR "desc" LIKE '%agel%'
),
jp_orders2017 AS (
    SELECT orderid
    , customerid
    FROM {{ ref('stg_orders') }}
    WHERE year(ordered) = 2017
    AND customerid IN (SELECT customerid FROM jps)
),
coffee_jp_orders_2017 AS (
    SELECT DISTINCT orderid
    FROM {{ ref('stg_orders_items') }}
    WHERE sku IN (SELECT sku FROM coffee_and_bagels)
    AND orderid IN (SELECT orderid FROM jp_orders2017)
)
SELECT jps.customerid, jps.name, jps.phone
FROM jps INNER JOIN jp_orders2017 USING (customerid)
WHERE orderid IN (SELECT orderid FROM coffee_jp_orders_2017)
