SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=5000;

CREATE TEMPORARY MACRO TO_STRING(x STRING)
  IF(TRIM(x) == '' OR TRIM(x) IS NULL, NULL, UCASE(TRIM(x)))
;

CREATE TEMPORARY MACRO TO_DOUBLE(x STRING)
  IF(TRIM(x) == '' OR TRIM(x) IS NULL, NULL, CAST(TRIM(x) AS DOUBLE))
;

CREATE TEMPORARY MACRO TO_BIGINT(x STRING)
  IF(TRIM(x) == '' OR TRIM(x) IS NULL, NULL, CAST(TRIM(x) AS BIGINT))
;

CREATE TEMPORARY MACRO TO_TIMESTAMP(x STRING)
  IF(TRIM(x) == '' OR TRIM(x) IS NULL,
      NULL,
      FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(x), 'yyyy-MM-dd HH:mm:ss')))
;

CREATE DATABASE IF NOT EXISTS raw;

CREATE EXTERNAL TABLE IF NOT EXISTS raw.traffic_accidents (
    incident_id             STRING,
    offense_id              STRING,
    offense_code            STRING,
    offense_code_extension  STRING,
    offense_type_id         STRING,
    offense_category_id     STRING,
    first_occurrence_date   STRING,
    last_occurrence_date    STRING,
    reported_date           STRING,
    incident_address        STRING,
    geo_x                   STRING,
    geo_y                   STRING,
    geo_lon                 STRING,
    geo_lat                 STRING,
    district_id             STRING,
    precinct_id             STRING,
    neighborhood_id         STRING,
    bicycle_ind             STRING,
    pedestrian_ind          STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 's3://nmcintyre-hive/tables/raw/traffic_accidents'
TBLPROPERTIES(
    "skip.header.line.count"="1"
)
;

CREATE EXTERNAL TABLE IF NOT EXISTS default.traffic_accidents (
  incident_id             BIGINT,
  offense_id              BIGINT,
  offense_code            BIGINT,
  offense_code_extension  BIGINT,
  offense_type_id         STRING,
  offense_category_id     STRING,
  first_occurrence_date   TIMESTAMP,
  last_occurrence_date    TIMESTAMP,
  reported_date           TIMESTAMP,
  incident_address        STRING,
  geo_x                   BIGINT,
  geo_y                   BIGINT,
  geo_lon                 DOUBLE,
  geo_lat                 DOUBLE,
  district_id             BIGINT,
  precinct_id             BIGINT,
  neighborhood_id         STRING,
  bicycle_ind             BOOLEAN,
  pedestrian_ind          BOOLEAN
)
PARTITIONED BY (day DATE)
STORED AS PARQUET
LOCATION 's3://nmcintyre-hive/tables/parquet/traffic_accidents'
;

INSERT OVERWRITE TABLE default.traffic_accidents
PARTITION (day)
SELECT
  TO_BIGINT(incident_id)                      AS incident_id,
  TO_BIGINT(offense_id)                       AS offense_id,
  TO_BIGINT(offense_code)                     AS offense_code,
  TO_BIGINT(offense_code_extension)           AS offense_code_extension,
  TO_STRING(offense_type_id)                  AS offense_type_id,
  TO_STRING(offense_category_id)              AS offense_category_id,
  TO_TIMESTAMP(first_occurrence_date)         AS first_occurrence_date,
  TO_TIMESTAMP(last_occurrence_date)          AS last_occurrence_date,
  TO_TIMESTAMP(reported_date)                 AS reported_date,
  TO_STRING(incident_address)                 AS incident_address,
  TO_BIGINT(geo_x)                            AS geo_x,
  TO_BIGINT(geo_y)                            AS geo_y,
  TO_DOUBLE(geo_lon)                          AS geo_lon,
  TO_DOUBLE(geo_lat)                          AS geo_lat,
  TO_BIGINT(district_id)                      AS district_id,
  TO_BIGINT(precinct_id)                      AS precinct_id,
  TO_STRING(neighborhood_id)                  AS neighborhood_id,
  IF(TRIM(bicycle_ind) = "1",    TRUE, FALSE) AS bicycle_ind,
  IF(TRIM(pedestrian_ind) = "1", TRUE, FALSE) AS pedestrian_ind,
  DATE(CAST(reported_date AS TIMESTAMP))      AS day
FROM
  raw.traffic_accidents
;