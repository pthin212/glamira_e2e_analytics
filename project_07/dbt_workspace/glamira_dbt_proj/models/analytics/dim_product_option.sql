{{ config(
    tags=["dimension"]
) }}

SELECT
    re.product_option_id,
    re.option_label,
    re.value_label,
FROM (
    SELECT 
        SHA256(CONCAT(option_label, value_label)) AS product_option_id,
        o.option_label,
        o.value_label,
        ROW_NUMBER() OVER (PARTITION BY SHA256(CONCAT(o.option_label, o.value_label))) AS row_num
    FROM {{ source('glamira_data', 'stg_cart_product') }} cp, 
        UNNEST(option) AS o
) AS re
WHERE row_num = 1