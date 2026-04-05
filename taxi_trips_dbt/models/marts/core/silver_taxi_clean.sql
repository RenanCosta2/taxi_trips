SELECT
    CAST(VendorID AS INT) AS VendorID,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
    CAST(passenger_count AS INT) AS passenger_count,
    CAST(trip_distance AS DOUBLE) AS trip_distance,
    CAST(speed_mph AS DOUBLE) AS speed_mph,
    CAST(pickup_longitude AS DOUBLE) AS pickup_longitude,
    CAST(pickup_latitude AS DOUBLE) AS pickup_latitude,
    CAST(RateCodeID AS INT) RateCodeID,
    CAST(store_and_fwd_flag AS CHAR) AS store_and_fwd_flag,
    CAST(dropoff_longitude AS DOUBLE) AS dropoff_longitude,
    CAST(dropoff_latitude AS DOUBLE) AS dropoff_latitude,
    CAST(payment_type AS INT) AS payment_type,
    CAST(fare_amount AS DOUBLE) AS fare_amount,
    CAST(extra AS FLOAT) AS extra,
    CAST(mta_tax AS FLOAT) AS mta_tax,
    CAST(tip_amount AS DOUBLE) AS tip_amount,
    CAST(tolls_amount AS DOUBLE) AS tolls_amount,
    CAST(improvement_surcharge AS FLOAT) AS improvement_surcharge,
    CAST(total_amount AS DOUBLE) AS total_amount,
    CAST(trip_duration_min AS INT) AS trip_duration_min,
    CAST(geo_distance_km AS DOUBLE) AS geo_distance_km
FROM
    {{ref('int_taxi_quality')}}
WHERE
    quality_score = 0