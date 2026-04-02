WITH source AS (
    SELECT 
        VendorID,
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime, 
        CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime, 
        passenger_count, 
        trip_distance, 
        pickup_longitude, 
        pickup_latitude, 
        RateCodeID, 
        store_and_fwd_flag, 
        dropoff_longitude, 
        dropoff_latitude, 
        payment_type, 
        fare_amount, 
        extra, 
        mta_tax, 
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    FROM 
    	{{ref('bronze_taxi')}}
),

-- filtro básico (nulls + regras simples)
clean_base AS (
    SELECT 
        *,

        -- NULL checks
        CASE 
	        WHEN VendorID IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_vendor,
        CASE 
	        WHEN tpep_pickup_datetime IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_pickup,
        CASE 
	        WHEN tpep_dropoff_datetime IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_dropoff,
        CASE 
	        WHEN passenger_count IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_passenger,
        CASE 
	        WHEN trip_distance IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_distance,
        CASE 
	        WHEN pickup_longitude IS NULL OR pickup_latitude IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_pickup_geo,
        CASE 
	        WHEN dropoff_longitude IS NULL OR dropoff_latitude IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_dropoff_geo,
        CASE
        	WHEN payment_type IS NULL THEN 1
        	ELSE 0
        END AS flag_null_payment_type,
        CASE 
	        WHEN fare_amount IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_fare,
        CASE 
	        WHEN extra IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_extra,
        CASE 
	        WHEN mta_tax IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_mta_tax,
        CASE 
	        WHEN tip_amount IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_tip,
        CASE 
	        WHEN tolls_amount IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_tolls,
        CASE 
	        WHEN improvement_surcharge IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_improvement_surcharge,
        CASE 
	        WHEN total_amount IS NULL THEN 1 
	        ELSE 0 
        END AS flag_null_total,

        -- regra de negócio
        CASE
        	WHEN VendorID NOT IN (1, 2) THEN 1
        	ELSE 0
        END AS flag_vendorid_invalid,
        CASE 
            WHEN passenger_count = 0 OR passenger_count > 6 THEN 1 
            ELSE 0 
        END AS flag_passenger_invalid,
        CASE
        	WHEN mta_tax NOT IN (0.5, 0) THEN 1
        	ELSE 0
        END AS flag_mta_tax_invalid,
        CASE 
        	WHEN extra NOT IN (0, 0.5, 1) THEN 1
        	ELSE 0
        END AS flag_extra_invalid,
        CASE
        	WHEN improvement_surcharge NOT IN (0.3) THEN 1
        	ELSE 0
        END AS flag_improvement_surcharge_invalid,
        CASE
        	WHEN RateCodeID NOT IN (1, 2, 3, 4, 5, 6) THEN 1
        	ELSE 0
        END AS flag_ratecode_invalid,
        CASE
        	WHEN store_and_fwd_flag NOT IN ('N', 'Y') THEN 1
        	ELSE 0
        END AS flag_store_and_fwd_flag_invalid,
        CASE
        	WHEN payment_type NOT IN (1, 2, 3, 4, 5, 6) THEN 1
        	ELSE 0
        END AS flag_payment_type_invalid
        
    FROM source
),

trip_metrics AS (
    SELECT *,
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_min,
		trip_distance / (trip_duration_min / 60.0) AS speed_mph
    FROM clean_base
),

statistics_stats AS (
	SELECT * FROM {{ ref('int_taxi_stats') }}
),

bounds AS (
    SELECT 
        *,
        (fare_q3 - fare_q1) AS fare_iqr,
        (fare_q1 - 1.5 * (fare_q3 - fare_q1)) AS fare_lower,
        (fare_q3 + 1.5 * (fare_q3 - fare_q1)) AS fare_upper,

        (tip_q3 - tip_q1) AS tip_iqr,
        (tip_q1 - 1.5 * (tip_q3 - tip_q1)) AS tip_lower,
        (tip_q3 + 1.5 * (tip_q3 - tip_q1)) AS tip_upper,

        (tolls_q3 - tolls_q1) AS tolls_iqr,
        (tolls_q1 - 1.5 * (tolls_q3 - tolls_q1)) AS tolls_lower,
        (tolls_q3 + 1.5 * (tolls_q3 - tolls_q1)) AS tolls_upper,

        (total_q3 - total_q1) AS total_iqr,
        (total_q1 - 1.5 * (total_q3 - total_q1)) AS total_lower,
        (total_q3 + 1.5 * (total_q3 - total_q1)) AS total_upper,

        (duration_q3 - duration_q1) AS duration_iqr,
        (duration_q1 - 1.5 * (duration_q3 - duration_q1)) AS duration_lower,
        (duration_q3 + 1.5 * (duration_q3 - duration_q1)) AS duration_upper

    FROM statistics_stats
),

outlier_flags AS (
	SELECT
		*,
		CASE
			WHEN fare_amount < 0 THEN 1
			WHEN fare_amount < fare_lower OR fare_amount > fare_upper THEN 1
			ELSE 0
		END AS flag_fare_outlier,
		
		CASE
			WHEN tip_amount < 0 THEN 1
			WHEN tip_amount < tip_lower OR tip_amount > tip_upper THEN 1
			ELSE 0
		END AS flag_tip_outlier,
		
		CASE
			WHEN tolls_amount < 0 THEN 1
			WHEN tolls_amount < tolls_lower OR tolls_amount > tolls_upper THEN 1
			ELSE 0
		END AS flag_tolls_outlier,
		
		CASE
			WHEN total_amount < 0 THEN 1
			WHEN total_amount < total_lower OR total_amount > total_upper THEN 1
			WHEN ABS(
				  total_amount - (
				    fare_amount 
				    + tip_amount 
				    + tolls_amount 
				    + extra 
				    + mta_tax 
				    + improvement_surcharge
				  )
				) >= 0.01 THEN 1
			ELSE 0
		END AS flag_total_outlier,
		
		CASE
			WHEN trip_duration_min <= 0 THEN 1
		    WHEN trip_duration_min > 1440 THEN 1
		    WHEN trip_duration_min < duration_lower 
     			OR trip_duration_min > duration_upper THEN 1
     		ELSE 0
		END AS flag_duration_outlier,

		CASE
			WHEN speed_mph < 0 THEN 1
			WHEN speed_mph > 100 THEN 1
			ELSE 0
		END AS flag_speed_outlier
		
	FROM
		trip_metrics
	CROSS JOIN 
		bounds
),

-- cálculo geográfico (Haversine) para verificação de trip_distance
geo_calc AS (
    SELECT 
        *,
        6371 * 2 * asin(
		    sqrt(
		        sin(radians(dropoff_latitude - pickup_latitude) / 2)^2 +
		        cos(radians(pickup_latitude)) *
		        cos(radians(dropoff_latitude)) *
		        sin(radians(dropoff_longitude - pickup_longitude) / 2)^2
		    )
		) geo_distance_km
    FROM outlier_flags
),

trip_distance_check AS (
    SELECT 
    	*,
    	CASE 
		    WHEN ABS(trip_distance - (geo_distance_km * 0.621371)) >= 5 THEN 1 -- tolerância de diferença
		    ELSE 0
		END AS flag_geo_distance_invalid
    FROM 
    	geo_calc
),

duplicate_check AS (
    SELECT 
        *,

        ROW_NUMBER() OVER (
            PARTITION BY 
                VendorID,
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                total_amount,
                trip_distance
            ORDER BY total_amount DESC
        ) AS rn

    FROM trip_distance_check
),

flagging_duplicate AS (
	SELECT 
        *,

        CASE 
            WHEN rn > 1 THEN 1
            ELSE 0
        END AS flag_duplicate

    FROM duplicate_check
),


quality_data AS (
	SELECT
		*,
		(
			flag_null_vendor +
			flag_null_pickup +
			flag_null_dropoff +
			flag_null_passenger +
			flag_null_distance +
			flag_null_pickup_geo +
			flag_null_dropoff_geo +
			flag_null_payment_type +
			flag_null_fare +
			flag_null_extra +
			flag_null_mta_tax +
			flag_null_tip +
			flag_null_tolls +
			flag_null_improvement_surcharge +
			flag_null_total +
			flag_vendorid_invalid +
			flag_passenger_invalid +
			flag_mta_tax_invalid +
			flag_extra_invalid +
			flag_improvement_surcharge_invalid +
			flag_ratecode_invalid +
			flag_store_and_fwd_flag_invalid +
			flag_payment_type_invalid +
			flag_fare_outlier +
			flag_tip_outlier +
			flag_tolls_outlier +
			flag_total_outlier +
			flag_duration_outlier +
			flag_speed_outlier +
			flag_geo_distance_invalid +
			flag_duplicate
		) AS quality_score
	FROM
		flagging_duplicate
)


SELECT
	*
FROM
	quality_data