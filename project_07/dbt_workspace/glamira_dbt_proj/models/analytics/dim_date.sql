{{ config(
    tags=["dimension"]
) }}

SELECT
    sd.date_key,
    sd.full_date,
    sd.day_of_week,
    sd.day_of_week_short,
    sd.year_month,
    sd.month,
    sd.year,
    sd.year_number,
    sd.is_weekday_or_weekend
FROM {{ source('glamira_data', 'stg_date') }} AS sd