{{ config(
    tags=["fact"],
    materialized='incremental',
    strategy='append'
) }}

SELECT
    o.order_id,
    CAST(cp.product_id AS STRING) as product_id,
    d.date_key,
    g.ip_address,
    cp.USD_price,
    cp.amount AS quantity,
    cp.original_currency,
    dpo.product_option_id,
    o.timestamp
FROM
    {{ source('glamira_data', 'stg_cart_product') }} AS cp,
    UNNEST(cp.option) AS opt
JOIN
    {{ source('glamira_data', 'stg_order') }} AS o ON cp.order_id = o.order_id
JOIN
    {{ source('glamira_data', 'stg_date') }} AS d ON o.local_time_ymd = d.full_date
LEFT JOIN
    {{ source('glamira_data', 'stg_geo') }} AS g ON o.ip = g.ip_address
LEFT JOIN
    {{ source('glamira_data', 'dim_product_option') }} AS dpo ON opt.option_label = dpo.option_label AND opt.value_label = dpo.value_label
{% if is_incremental() %}
WHERE o.timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}