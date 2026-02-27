{{ config(materialized='table') }}

WITH features AS (
    SELECT * FROM {{ ref('int_flight_features') }}
),

airline_summary AS (
    SELECT
        'airline'                                          AS dimension,
        airline_iata                                      AS dimension_value,
        COUNT(*)                                          AS total_flights,
        SUM(is_delayed)                                   AS delayed_flights,
        ROUND((AVG(is_delayed::FLOAT) * 100)::NUMERIC, 1) AS delay_rate_pct,
        ROUND(AVG(departure_delay_min)::NUMERIC, 1)       AS avg_delay_min
    FROM features
    WHERE airline_iata IS NOT NULL
    GROUP BY airline_iata
),

airport_summary AS (
    SELECT
        'airport'                                          AS dimension,
        origin_airport                                    AS dimension_value,
        COUNT(*)                                          AS total_flights,
        SUM(is_delayed)                                   AS delayed_flights,
        ROUND((AVG(is_delayed::FLOAT) * 100)::NUMERIC, 1) AS delay_rate_pct,
        ROUND(AVG(departure_delay_min)::NUMERIC, 1)       AS avg_delay_min
    FROM features
    WHERE origin_airport IS NOT NULL
    GROUP BY origin_airport
),

hour_summary AS (
    SELECT
        'hour'                                             AS dimension,
        scheduled_hour::TEXT                              AS dimension_value,
        COUNT(*)                                          AS total_flights,
        SUM(is_delayed)                                   AS delayed_flights,
        ROUND((AVG(is_delayed::FLOAT) * 100)::NUMERIC, 1) AS delay_rate_pct,
        ROUND(AVG(departure_delay_min)::NUMERIC, 1)       AS avg_delay_min
    FROM features
    WHERE scheduled_hour IS NOT NULL
    GROUP BY scheduled_hour
),

month_summary AS (
    SELECT
        'month'                                            AS dimension,
        month::TEXT                                       AS dimension_value,
        COUNT(*)                                          AS total_flights,
        SUM(is_delayed)                                   AS delayed_flights,
        ROUND((AVG(is_delayed::FLOAT) * 100)::NUMERIC, 1) AS delay_rate_pct,
        ROUND(AVG(departure_delay_min)::NUMERIC, 1)       AS avg_delay_min
    FROM features
    WHERE month IS NOT NULL
    GROUP BY month
)

SELECT * FROM airline_summary
UNION ALL
SELECT * FROM airport_summary
UNION ALL
SELECT * FROM hour_summary
UNION ALL
SELECT * FROM month_summary

ORDER BY dimension, delay_rate_pct DESC