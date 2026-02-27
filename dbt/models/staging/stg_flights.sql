-- stg_flights.sql
-- Cleans raw flights data — fixes types, names, adds ML target column
-- Materialized as VIEW (no storage cost, always fresh)

{{ config(materialized='view') }}
WITH source AS (
    SELECT * FROM {{ source('raw', 'flights') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        TRIM(airline)                AS airline_iata,
        flight_number,
        TRIM(tail_number)            AS tail_number,
        TRIM(origin_airport)         AS origin_airport,
        TRIM(destination_airport)    AS destination_airport,

        -- Date parts
        year::INT                    AS year,
        month::INT                   AS month,
        day::INT                     AS day,
        day_of_week::INT             AS day_of_week,
        FLOOR(
            COALESCE(scheduled_departure::NUMERIC, 0) / 100
        )::INT                       AS scheduled_hour,

        -- Delays
        departure_delay::FLOAT       AS departure_delay_min,
        arrival_delay::FLOAT         AS arrival_delay_min,
        taxi_out::FLOAT              AS taxi_out_min,
        air_time::FLOAT              AS air_time_min,
        distance::FLOAT              AS distance_miles,

        -- Delay cause breakdown
        weather_delay::FLOAT         AS weather_delay_min,
        airline_delay::FLOAT         AS airline_delay_min,
        late_aircraft_delay::FLOAT   AS late_aircraft_delay_min,
        air_system_delay::FLOAT      AS air_system_delay_min,

        -- Status
        cancelled::INT               AS is_cancelled,
        diverted::INT                AS is_diverted,

        -- ML TARGET: 1 if delayed 15+ minutes (FAA definition)
        CASE
            WHEN departure_delay >= 15 THEN 1
            ELSE 0
        END                          AS is_delayed,

        -- Regression target: actual delay minutes (0 if early/on-time)
        GREATEST(
            COALESCE(departure_delay::FLOAT, 0), 0
        )                            AS delay_minutes

    FROM source
    WHERE
        cancelled = 0              -- Remove cancelled flights
        AND year IS NOT NULL
        AND month BETWEEN 1 AND 12
        AND day BETWEEN 1 AND 31
)

SELECT * FROM cleaned