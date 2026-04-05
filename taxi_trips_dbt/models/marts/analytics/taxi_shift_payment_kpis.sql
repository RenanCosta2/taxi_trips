SELECT 
    shift,

    CASE 
        WHEN payment_type = 1 THEN 'Cartão de crédito'
        WHEN payment_type = 2 THEN 'Dinheiro'
        WHEN payment_type = 3 THEN 'Sem cobrança'
        WHEN payment_type = 4 THEN 'Contestação'
        WHEN payment_type = 5 THEN 'Desconhecido'
        WHEN payment_type = 6 THEN 'Viagem cancelada'
    END AS payment_type_name,

    COUNT(*) AS total_trips,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(total_amount) AS avg_ticket,
    SUM(total_amount) AS total_revenue

FROM 
	{{ref('silver_taxi_enriched')}}
GROUP BY 
    shift,
    payment_type,
    payment_type_name