{{ config(
    tags=["staging"]
) }}

SELECT
    er.original_currency_representation AS original_currency,
    er.currency_code,
    er.currency_symbol,
    er.usd_exchange_rate,
FROM {{ source('glamira_data', 'exchange_rates') }} AS er