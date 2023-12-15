WITH p1 AS (
    SELECT customerid
    , name
    , phone
    FROM {{ ref('stg_customers') }}
    WHERE phone_numberize(split_part(name, ' ', 2)) = replace(phone, '-', '')
)
SELECT * FROM p1