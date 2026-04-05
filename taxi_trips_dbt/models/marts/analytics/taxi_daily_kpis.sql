SELECT 
	CAST(tpep_pickup_datetime AS DATE) AS date,
	COUNT(*) AS total_trips,
	AVG(fare_amount) AS avg_fare,
	AVG(tip_amount) AS avg_tip,
    AVG(total_amount) AS avg_ticket,
	SUM(total_amount) AS total_revenue
FROM 
	{{ref('silver_taxi_enriched')}}
GROUP BY
	CAST(tpep_pickup_datetime AS DATE)