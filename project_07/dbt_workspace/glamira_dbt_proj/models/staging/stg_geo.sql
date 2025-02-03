{{ config(
    tags=["staging"]
) }}

SELECT
    uil.ip_address,
    uil.country_code,
    uil.country_name,
    uil.region,
    uil.city
FROM {{ source('glamira_data', 'user_ip_locations') }} AS uil
WHERE uil.ip_address != '-' 
    AND uil.country_code != '-'
    AND uil.country_code != 'IPV6 ADDRESS MISSING IN IPV4 BIN'
    AND uil.country_name != '-'
    AND uil.region != '-'
    AND uil.city != '-'