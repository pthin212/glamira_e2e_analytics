{{ config(
    tags=["staging"]
) }}

SELECT
    scs.record_id,
    scs.order_id,
    scs.timestamp,
    scs.local_time,
    FORMAT_DATE('%Y-%m-%d', scs.local_time) AS local_time_ymd,
    scs.store_id,
    scs.user_id_db,
    scs.user_agent,
    scs.resolution,
    scs.email_address,
    scs.device_id,
    scs.ip,
FROM {{ source('glamira_data', 'stg_checkout_source') }} AS scs
WHERE event_collection = 'checkout_success'