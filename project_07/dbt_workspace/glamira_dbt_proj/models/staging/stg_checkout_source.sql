{{ config(
    tags=["checkout_source"]
) }}

WITH RankedEvents AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY timestamp DESC) as rn 
    FROM {{ source('glamira_data', 'raw_events') }}
    WHERE event_collection = 'checkout_success'
)
SELECT 
    re.record_id,
    re.event_collection,
    re.timestamp,
    re.ip,
    re.user_agent,
    re.resolution,
    re.user_id_db,
    re.device_id,
    re.api_version,
    re.store_id,
    re.local_time,
    re.show_recommendation,
    re.current_url,
    re.referrer_url,
    re.email_address,
    re.order_id,
    ARRAY_AGG(
        STRUCT(
        cp.product_id,
        cp.amount,
        cp.price,
        cp.currency,
        CASE
            WHEN ARRAY_LENGTH(cp.option) = 0 
                THEN ARRAY[STRUCT<option_label STRING, option_id STRING, value_label STRING, value_id STRING, quality STRING, quality_label STRING>("N/A", "N/A", "N/A", "N/A", null, null)]
            ELSE cp.option
        END AS option
        )
    ) AS cart_products,
FROM RankedEvents re,
    UNNEST(re.cart_products) AS cp
WHERE rn = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16