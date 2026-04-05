SELECT 
	COUNT(*) AS total_trips,
	AVG(trip_distance) AS avg_mile_distance,
	AVG(trip_duration_min) AS avg_duration_min,
	AVG(speed_mph) AS avg_speed,
    SUM(CASE WHEN passenger_count = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS pct_single_passenger,
	AVG(fare_amount) AS avg_fare,
	AVG(tip_amount) AS avg_tip,
	AVG(total_amount) AS avg_revenue,
	SUM(total_amount) AS total_revenue
FROM 
	{{ref('silver_taxi_enriched')}}