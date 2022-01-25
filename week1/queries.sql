-- Question 3
SELECT count(1)
	FROM public.yellow_taxi_trips
	WHERE date(tpep_pickup_datetime) = date('2021-01-15');

-- Question 4
-- Largest tip for each day
SELECT date(tpep_pickup_datetime), max(tip_amount) as largest_tip_of_a_day
	FROM public.yellow_taxi_trips
	GROUP BY date(tpep_pickup_datetime)
	ORDER BY date(tpep_pickup_datetime);

-- Largest tip in January
SELECT max(tip_amount) as largest_tip_jan
	FROM public.yellow_taxi_trips
	WHERE
		EXTRACT(MONTH from date(tpep_pickup_datetime)) = 1;

-- Largest tip January 2021
SELECT max(tip_amount) as largest_tip_jan
	FROM public.yellow_taxi_trips
	WHERE
		EXTRACT(MONTH from date(tpep_pickup_datetime)) = 1
		AND EXTRACT(YEAR from date(tpep_pickup_datetime)) = 2021;

-- Question 5
SELECT dropoff_zone."Zone" as destination, count(1) as number_of_rides
	FROM public.yellow_taxi_trips
	LEFT JOIN public.zones as pickup_zone
		ON "PULocationID" = pickup_zone."LocationID"
	LEFT JOIN public.zones as dropoff_zone
		ON "DOLocationID" = dropoff_zone."LocationID"
	WHERE date(tpep_pickup_datetime) = date('2021-01-14')
		AND pickup_zone."Zone" ILIKE 'central park'
	GROUP BY dropoff_zone."Zone"
	ORDER BY number_of_rides DESC
	LIMIT 1;

-- Question 6
SELECT pickup_zone."Zone" as pickup, dropoff_zone."Zone" as destination, avg(total_amount) as avg_total
	FROM public.yellow_taxi_trips
	LEFT JOIN public.zones as pickup_zone
		ON "PULocationID" = pickup_zone."LocationID"
	LEFT JOIN public.zones as dropoff_zone
		ON "DOLocationID" = dropoff_zone."LocationID"
	GROUP BY pickup_zone."Zone", dropoff_zone."Zone"
	ORDER BY avg_total DESC
	LIMIT 1;