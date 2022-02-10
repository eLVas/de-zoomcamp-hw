-- Question 1
SELECT COUNT(*)
FROM
  `data-zoomcamp-339218.trips_data_all.fhv_trips_q3`
WHERE
  EXTRACT(YEAR FROM DATE(dropoff_datetime)) >= 2019;


-- Question 2
SELECT COUNT(DISTINCT dispatching_base_num)
FROM
  `data-zoomcamp-339218.trips_data_all.fhv_trips_q3`
WHERE
  EXTRACT(YEAR FROM DATE(dropoff_datetime)) >= 2019;


-- Question 4
SELECT COUNT(*)
FROM
  `data-zoomcamp-339218.trips_data_all.fhv_trips_q3`
WHERE
  DATE(dropoff_datetime) >= "2019-01-01"
  AND DATE(dropoff_datetime) < "2019-03-31"
  AND dispatching_base_num IN ("B00987", "B02060", "B02279");


