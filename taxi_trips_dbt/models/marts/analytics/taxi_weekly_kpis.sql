SELECT 
    CASE 
        WHEN day_of_week = 1 THEN 'Segunda'
        WHEN day_of_week = 2 THEN 'Terça'
        WHEN day_of_week = 3 THEN 'Quarta'
        WHEN day_of_week = 4 THEN 'Quinta'
        WHEN day_of_week = 5 THEN 'Sexta'
        WHEN day_of_week = 6 THEN 'Sábado'
        WHEN day_of_week = 7 THEN 'Domingo'
    END AS day_of_week,

    COUNT(*) AS total_trips,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(total_amount) AS avg_ticket,
    SUM(total_amount) AS total_revenue
FROM 
	{{ref('silver_taxi_enriched')}}
GROUP BY 
	day_of_week