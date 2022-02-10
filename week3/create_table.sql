-- Create yellow taxi trips table
CREATE OR REPLACE TABLE
  `data-zoomcamp-339218.trips_data_all.yellow_taxi_trips`
PARTITION BY
  DATE(tpep_pickup_datetime)
CLUSTER BY
  VendorID,
  PULocationID,
  DOLocationID AS
SELECT
  *
FROM
  `data-zoomcamp-339218.trips_data_all.yellow_taxi_trips_external`;

-- Create fhv trips table
CREATE OR REPLACE TABLE
  `data-zoomcamp-339218.trips_data_all.fhv_trips`
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  dispatching_base_num,
  PULocationID,
  DOLocationID AS
SELECT
  *
FROM
  `data-zoomcamp-339218.trips_data_all.fhv_trips_external`;

-- Create fhv trips table Q3
CREATE OR REPLACE TABLE
  `data-zoomcamp-339218.trips_data_all.fhv_trips_q3`
PARTITION BY
  DATE(dropoff_datetime)
CLUSTER BY
  dispatching_base_num AS
SELECT
  *
FROM
  `data-zoomcamp-339218.trips_data_all.fhv_trips_external`;

-- Create fhv trips table Q5
CREATE OR REPLACE TABLE
  `data-zoomcamp-339218.trips_data_all.fhv_trips_q5`
CLUSTER BY
  dispatching_base_num, SR_Flag AS
SELECT
  *
FROM
  `data-zoomcamp-339218.trips_data_all.fhv_trips_external`;

