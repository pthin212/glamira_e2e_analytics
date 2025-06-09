{{ config(
    tags=["staging"]
) }}

SELECT
    re.record_id,
    re.order_id,
    re.product_id,
    re.amount,
    re.USD_price,
    re.original_currency,
    re.option
FROM (
    SELECT
        t1.record_id,
        t1.order_id,
        cp.product_id,
        cp.amount,
        ROUND( 
            -- Safely cast the processed price string to a numeric type; returns NULL if conversion fails
            SAFE_CAST( 
                -- Remove unwanted characters (',', '.', '-', single quotes)
                REGEXP_REPLACE( 
                    -- Handle cases where the price might be missing cents (adds ".00" if necessary)
                    CASE
                        WHEN REGEXP_CONTAINS(SUBSTR(cp.price, LENGTH(cp.price)-2, 3), r'^[0-9]{3}$') THEN CONCAT(cp.price, '.00')
                        ELSE cp.price
                    END, 
                    -- Regular expression to remove ',','.','-', and single quotes.
                    r"['-.,,]", ""
                -- Multiply by the USD exchange rate (/100.0 because the price, before cleaning non-numeric characters, is assumed to be in the format %.yz)
                ) AS NUMERIC) * (er.usd_exchange_rate/100.0), 3
        ) AS USD_price,
        TRIM(cp.currency) as original_currency,
        ARRAY(
            SELECT AS STRUCT
                option_label,
                option_id,
                value_label,
                value_id
            FROM UNNEST(cp.option)
        ) AS option
    FROM
        {{ source('glamira_data', 'stg_checkout_source') }} AS t1,
        UNNEST(t1.cart_products) AS cp
        LEFT JOIN {{ source('glamira_data', 'exchange_rates') }} AS er ON TRIM(cp.currency) = er.original_currency_representation
    WHERE event_collection = 'checkout_success'
        AND cp.currency IS NOT NULL
        AND TRIM(cp.currency) != ''
) AS re
WHERE USD_price IS NOT NULL