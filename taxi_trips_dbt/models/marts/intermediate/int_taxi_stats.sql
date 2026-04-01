WITH base AS (
    SELECT *,
        datediff('minute', CAST(tpep_pickup_datetime AS TIMESTAMP), CAST(tpep_dropoff_datetime AS TIMESTAMP)) AS trip_duration_min
    FROM {{ ref('bronze_taxi') }}
)

SELECT
    approx_quantile(fare_amount, 0.25) AS fare_q1,
    approx_quantile(fare_amount, 0.75) AS fare_q3,

    approx_quantile(tip_amount, 0.25) AS tip_q1,
    approx_quantile(tip_amount, 0.75) AS tip_q3,

    approx_quantile(tolls_amount, 0.25) AS tolls_q1,
    approx_quantile(tolls_amount, 0.75) AS tolls_q3,

    approx_quantile(total_amount, 0.25) AS total_q1,
    approx_quantile(total_amount, 0.75) AS total_q3,

    approx_quantile(trip_duration_min, 0.25) AS duration_q1,
    approx_quantile(trip_duration_min, 0.75) AS duration_q3
FROM base