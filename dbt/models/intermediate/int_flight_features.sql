-- int_flight_features.sql
-- Builds historical aggregate features for ML model
-- Materialized as TABLE (expensive to compute, query often)

{{ config(materialized='table') }}

WITH flights AS (
    SELECT * FROM {{ ref('stg_flights') }}
),

-- Delay rate per airline
airline_stats AS (
    SELECT
        airline_iata,
        AVG(is_delayed::FLOAT)       AS airline_delay_rate,
        AVG(departure_delay_min)     AS airline_avg_delay_min
    FROM flights
    GROUP BY airline_iata
),

-- Delay rate per origin airport
origin_stats AS (
    SELECT
        origin_airport,
        AVG(is_delayed::FLOAT)       AS origin_delay_rate,
        AVG(departure_delay_min)     AS origin_avg_delay_min
    FROM flights
    GROUP BY origin_airport
),

-- Delay rate per route
route_stats AS (
    SELECT
        origin_airport,
        destination_airport,
        AVG(is_delayed::FLOAT)       AS route_delay_rate,
        AVG(departure_delay_min)     AS route_avg_delay_min,
        AVG(air_time_min)            AS route_avg_air_time_min,
        COUNT(*)                     AS route_total_flights
    FROM flights
    GROUP BY origin_airport, destination_airport
),

-- Delay rate per hour of day
hour_stats AS (
    SELECT
        scheduled_hour,
        AVG(is_delayed::FLOAT)       AS hour_delay_rate
    FROM flights
    GROUP BY scheduled_hour
),

-- Delay rate per month
month_stats AS (
    SELECT
        month,
        AVG(is_delayed::FLOAT)       AS month_delay_rate
    FROM flights
    GROUP BY month
),

-- Delay rate per day of week
dow_stats AS (
    SELECT
        day_of_week,
        AVG(is_delayed::FLOAT)       AS dow_delay_rate
    FROM flights
    GROUP BY day_of_week
),

-- Join everything together
enriched AS (
    SELECT
        f.*,

        -- Airline features
        a.airline_delay_rate,
        a.airline_avg_delay_min,

        -- Origin airport features
        o.origin_delay_rate,
        o.origin_avg_delay_min,

        -- Route features
        r.route_delay_rate,
        r.route_avg_delay_min,
        r.route_avg_air_time_min,
        r.route_total_flights,

        -- Time features
        h.hour_delay_rate,
        m.month_delay_rate,
        d.dow_delay_rate,

        -- Derived features
        CASE
            WHEN f.scheduled_hour BETWEEN 6  AND 9  THEN 'morning'
            WHEN f.scheduled_hour BETWEEN 10 AND 14 THEN 'midday'
            WHEN f.scheduled_hour BETWEEN 15 AND 19 THEN 'afternoon'
            ELSE 'evening'
        END AS time_of_day,

        CASE
            WHEN f.day_of_week IN (6, 7) THEN 1 ELSE 0
        END AS is_weekend,

        CASE
            WHEN f.distance_miles < 500  THEN 'short'
            WHEN f.distance_miles < 1500 THEN 'medium'
            ELSE 'long'
        END AS distance_category

    FROM flights f
    LEFT JOIN airline_stats a USING (airline_iata)
    LEFT JOIN origin_stats  o USING (origin_airport)
    LEFT JOIN route_stats   r USING (origin_airport, destination_airport)
    LEFT JOIN hour_stats    h ON f.scheduled_hour = h.scheduled_hour
    LEFT JOIN month_stats   m ON f.month          = m.month
    LEFT JOIN dow_stats     d ON f.day_of_week    = d.day_of_week
)

SELECT * FROM enriched