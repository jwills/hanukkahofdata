WITH senior_cat_food_skus AS (
    SELECT sku
    FROM {{ ref('stg_products') }}
    WHERE "desc" LIKE '%Senior Cat Food%'
),
senior_cat_food_orders AS (
    SELECT orderid, COUNT(1) as cat_food_count
    FROM {{ ref('stg_orders_items') }} oi
    INNER JOIN senior_cat_food_skus cfs ON oi.sku = cfs.sku
    GROUP BY orderid
),
top_senior_cat_food_orders_by_customer AS (
    SELECT o.customerid, SUM(cat_food_count) as cat_food_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN senior_cat_food_orders cfo ON o.orderid = cfo.orderid
    GROUP BY o.customerid
)
SELECT c.customerid, c.name, c.phone
FROM {{ ref('stg_customers') }} c
INNER JOIN top_senior_cat_food_orders_by_customer tscfo ON c.customerid = tscfo.customerid
WHERE c.citystatezip LIKE 'Staten Island%'
ORDER BY cat_food_count DESC
LIMIT 1