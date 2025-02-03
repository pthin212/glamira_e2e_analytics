{{ config(
    tags=["staging"]
) }}

SELECT
    p.product_id,
    p.product_name,
    p.url
FROM {{ source('glamira_data', 'products') }} AS p
WHERE p.product_name != 'N/A'