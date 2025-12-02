SELECT a.airport_name, AVG(f.dep_delay) AS avg_delay
FROM FactFlightDelay f
JOIN DimAirport a ON f.origin_airport_id = a.airport_id
GROUP BY airport_name
ORDER BY avg_delay DESC
LIMIT 10;

SELECT al.airline_name, AVG(f.arr_delay) AS avg_arrival_delay
FROM FactFlightDelay f
JOIN DimAirline al ON f.airline_id = al.airline_id
GROUP BY al.airline_name
ORDER BY avg_arrival_delay DESC
LIMIT 10;

SELECT d.year, d.month, AVG(f.dep_delay) AS avg_delay
FROM FactFlightDelay f
JOIN DimDate d ON f.date_id = d.date_id
GROUP BY 1,2
ORDER BY 1,2;
