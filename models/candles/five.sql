WITH staten_island_customers AS (
    SELECT *
    FROM {{ ref('stg_customers') }}
    WHERE citystatezip LIKE 'Brooklyn%'
),
cat_food_skus AS (
    SELECT sku
    FROM {{ ref('stg_products') }}
    WHERE "desc" LIKE '%Senior Cat Food%'
),
cat_food_orders AS (
    SELECT orderid, COUNT(1) as cat_food_count
    FROM {{ ref('stg_orders_items') }} oi
    INNER JOIN cat_food_skus cfs ON oi.sku = cfs.sku
    GROUP BY orderid
),
cat_food_orders_by_customer AS (
    SELECT o.customerid, SUM(cat_food_count) as cat_food_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN cat_food_orders cfo ON o.orderid = cfo.orderid
    GROUP BY o.customerid
),
cat_food_orders_by_customer_with_staten_island AS (
    SELECT c.customerid, c.name, c.phone, cfo.cat_food_count
    FROM staten_island_customers c
    INNER JOIN cat_food_orders_by_customer cfo ON c.customerid = cfo.customerid
    ORDER BY cfo.cat_food_count DESC
)
SELECT c.customerid, c.name, c.phone, cfo.cat_food_count
FROM cat_food_orders_by_customer cfo
INNER JOIN {{ ref('stg_customers') }} c ON cfo.customerid = c.customerid
ORDER BY cfo.cat_food_count DESC
LIMIT 1