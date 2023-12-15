WITH customer6_orders AS (
    SELECT o.* FROM {{ ref('stg_orders') }} o
    INNER JOIN {{ ref('six') }} USING (customerid) 
),
customer6_color_orders_items AS (
    SELECT oi.*
    FROM {{ ref('stg_orders_items') }} oi
    INNER JOIN customer6_orders o ON oi.orderid = o.orderid
    WHERE sku LIKE 'COL%' or sku LIKE 'HOM%'
),
next_ordered_item AS (
    SELECT oi.*
    FROM customer6_color_orders_items c6oi
    ASOF JOIN {{ ref('stg_orders_items') }} oi ON c6oi.orderid > oi.orderid
    WHERE oi.sku LIKE 'COL%' or oi.sku LIKE 'HOM%'
    ORDER BY c6oi.orderid - oi.orderid
),
matching_order AS (
    SELECT *
    FROM {{ ref('stg_orders') }} o
    INNER JOIN next_ordered_item noi ON o.orderid = noi.orderid
)
SELECT c.customerid, c.name, c.phone
FROM {{ ref('stg_customers') }} c
INNER JOIN matching_order mo ON c.customerid = mo.customerid