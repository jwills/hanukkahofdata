WITH collectibles_skus AS (
    SELECT sku
    FROM {{ ref('stg_products') }}
    WHERE sku LIKE 'COL%'
),
collectibles_order_items AS (
    SELECT oi.*
    FROM {{ ref('stg_orders_items') }} oi
    INNER JOIN collectibles_skus cs ON oi.sku = cs.sku
),
top_collector_id AS (
    SELECT o.customerid
    , COUNT(DISTINCT oi.sku) AS collectibles_count
    FROM {{ ref('stg_orders') }} o
    INNER JOIN collectibles_order_items oi ON o.orderid = oi.orderid
    GROUP BY o.customerid
    ORDER BY collectibles_count DESC
)
SELECT c.customerid, c.name, c.phone
FROM {{ ref('stg_customers') }} c
INNER JOIN top_collector_id tci ON c.customerid = tci.customerid

