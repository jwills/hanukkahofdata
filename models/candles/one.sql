WITH p1 AS (
    SELECT customerid
    , name
    , phone
    , split_part(name, ' ', 2) as last_name
    , split_part(phone, '-', 1) as area_code
    , split_part(phone, '-', 2) as first_three_digits
    , split_part(phone, '-', 3) as last_four_digits
    FROM {{ ref('stg_customers') }}
)
SELECT customerid, name, phone FROM p1
WHERE (phone_numberize(last_name) = area_code || first_three_digits || last_four_digits)
OR (phone_numberize(last_name) = first_three_digits || last_four_digits)
