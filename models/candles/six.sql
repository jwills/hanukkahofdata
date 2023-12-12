WITH order_profit_cost AS (
    SELECT orderid
    , SUM(qty * unit_price) as profit
    , SUM(qty * wholesale_cost) as cost
    FROM  {{ ref('stg_orders_items') }}
    INNER JOIN {{ ref('stg_products') }} USING (sku)
    GROUP BY ALL
),
unprofitable_customers AS (
    SELECT c.customerid, c.name, c.phone, COUNT(1)
    FROM {{ ref('stg_customers') }} c
    INNER JOIN {{ ref('stg_orders')}} o USING (customerid)
    INNER JOIN order_profit_cost opc USING (orderid)
    WHERE opc.profit < opc.cost
    GROUP BY ALL
    ORDER BY COUNT(1) DESC
)
SELECT * FROM unprofitable_customers LIMIT 1
