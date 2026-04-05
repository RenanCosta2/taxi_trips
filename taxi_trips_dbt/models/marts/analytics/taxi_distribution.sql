SELECT 
    quantile_cont(trip_duration_min, 0.5) AS p50_duration,
    quantile_cont(trip_duration_min, 0.9) AS p90_duration,
    quantile_cont(trip_duration_min, 0.95) AS p95_duration,

    quantile_cont(trip_distance, 0.5) AS p50_distance,
    quantile_cont(trip_distance, 0.9) AS p90_distance,
    quantile_cont(trip_distance, 0.95) AS p95_distance,

    quantile_cont(total_amount, 0.5) AS p50_value,
    quantile_cont(total_amount, 0.9) AS p90_value,
    quantile_cont(total_amount, 0.95) AS p95_value

FROM 
    {{ref('silver_taxi_enriched')}}