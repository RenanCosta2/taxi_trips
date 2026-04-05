{{ config(materialized='table') }}

SELECT 
    VendorID,

    tpep_pickup_datetime,
    CAST(tpep_pickup_datetime AS DATE) AS date,

    day_of_week,
    CASE 
        WHEN day_of_week = 1 THEN 'Segunda'
        WHEN day_of_week = 2 THEN 'Terça'
        WHEN day_of_week = 3 THEN 'Quarta'
        WHEN day_of_week = 4 THEN 'Quinta'
        WHEN day_of_week = 5 THEN 'Sexta'
        WHEN day_of_week = 6 THEN 'Sábado'
        WHEN day_of_week = 7 THEN 'Domingo'
    END AS day_name,

    trip_distance,
    trip_duration_min,
    trip_hour,
    speed_mph,
    passenger_count,

    fare_amount,
    tip_amount,
    total_amount,

    shift,

    payment_type,
    CASE 
        WHEN payment_type = 1 THEN 'Cartão de crédito'
        WHEN payment_type = 2 THEN 'Dinheiro'
        WHEN payment_type = 3 THEN 'Sem cobrança'
        WHEN payment_type = 4 THEN 'Contestação'
        WHEN payment_type = 5 THEN 'Desconhecido'
        WHEN payment_type = 6 THEN 'Viagem cancelada'
        ELSE 'Outro'
    END AS payment_type_name,

    CONCAT(ROUND(pickup_latitude, 2), '_', ROUND(pickup_longitude, 2)) AS origin,
    CONCAT(ROUND(dropoff_latitude, 2), '_', ROUND(dropoff_longitude, 2)) AS destination

FROM {{ ref('silver_taxi_enriched') }}