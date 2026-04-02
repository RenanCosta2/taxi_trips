WITH source AS (
	SELECT 
		VendorID, 
		tpep_pickup_datetime, 
		EXTRACT(HOUR FROM tpep_pickup_datetime) AS trip_hour,
		tpep_dropoff_datetime, 
		passenger_count, 
		trip_distance, 
		speed_mph,
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
		total_amount, 
		trip_duration_min, 
		geo_distance_km
	FROM 
		{{ref('silver_taxi_clean')}}
),

new_columns AS (
	SELECT
		*,
		
		CASE
			WHEN trip_distance >= 20 THEN 'longa'
			WHEN trip_distance >= 10 THEN 'média'
			ELSE 'curta'
		END AS distance_category,
		
		CASE
			WHEN trip_duration_min >= 25 THEN 'longa'
			WHEN trip_duration_min >= 5 THEN 'normal'
			ELSE 'curta'
		END AS trip_duration_category,
		
		CASE
			WHEN trip_hour < 6 THEN 'madrugada'
			WHEN trip_hour < 12 THEN 'manhã'
			WHEN trip_hour < 18 THEN 'tarde'
			ELSE 'noite'
		END AS shift,
		
		(total_amount / trip_duration_min) AS revenue_per_min,
		(total_amount / trip_distance) AS revenue_per_mile,
		
		CASE 
		    WHEN EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) IN (0, 6) THEN 1
		    ELSE 0
		END AS is_weekend
		
	FROM
		source

)

SELECT
	*
FROM
	new_columns