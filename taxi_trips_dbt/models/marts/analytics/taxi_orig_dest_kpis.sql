SELECT 
	CONCAT(ROUND(pickup_latitude, 2), '_', ROUND(pickup_longitude, 2)) AS origin,
    CONCAT(ROUND(dropoff_latitude, 2), '_', ROUND(dropoff_longitude, 2)) AS destination,
    COUNT(*) AS total_trips,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(total_amount) AS avg_ticket,
    SUM(total_amount) AS total_revenue

FROM 
	{{ref('silver_taxi_enriched')}}
GROUP BY 
    origin,
    destination
HAVING COUNT(*) > 100