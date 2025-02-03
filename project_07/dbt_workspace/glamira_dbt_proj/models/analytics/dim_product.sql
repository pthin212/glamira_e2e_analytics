{{ config(
    tags=["dimension"]
) }}

SELECT
    sp.product_id,
    sp.product_name,
    sp.url
FROM {{ source('glamira_data', 'stg_product') }} AS sp