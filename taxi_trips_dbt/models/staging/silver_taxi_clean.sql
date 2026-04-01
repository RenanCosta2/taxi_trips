SELECT
    *
FROM
    {{ref('int_taxi_quality')}}
WHERE
    quality_score = 0