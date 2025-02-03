{{ config(
    tags=["dimension"]
) }}

SELECT
    sg.ip_address,
    sg.country_code,
    sg.country_name,
    sg.region,
    sg.city
FROM  {{ source('glamira_data', 'stg_geo') }} AS sg