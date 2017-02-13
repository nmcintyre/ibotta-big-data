SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=500;

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
      FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(x), 'MM/dd/yyyy hh:mm:ss aa'), 'yyyy-MM-dd HH:mm:ss'))
;

CREATE TEMPORARY MACRO TO_DATE(x STRING)
  IF(TRIM(x) == '' OR TRIM(x) IS NULL,
      NULL,
      FROM_UNIXTIME(UNIX_TIMESTAMP(TRIM(x), 'MM/dd/yyyy'), 'yyyy-MM-dd'))
;

CREATE DATABASE IF NOT EXISTS raw;

CREATE EXTERNAL TABLE IF NOT EXISTS raw.service_requests (
    case_summary            STRING,
    case_status             STRING,
    case_source             STRING,
    case_created_date       STRING,
    case_created_dttm       STRING,
    case_closed_date        STRING,
    case_closed_dttm        STRING,
    first_call_resolution   STRING,
    customer_zip_code       STRING,
    incident_address_1      STRING,
    incident_address_2      STRING,
    incident_intersection_1 STRING,
    incident_intersection_2 STRING,
    incident_zip_code       STRING,
    longitude               STRING,
    latitude                STRING,
    agency                  STRING,
    division                STRING,
    major_area              STRING,
    type                    STRING,
    topic                   STRING,
    council_district        STRING,
    police_district         STRING,
    neighborhood            STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 's3://nmcintyre-hive/tables/raw/service_requests'
TBLPROPERTIES(
    "skip.header.line.count"="1"
)
;

CREATE EXTERNAL TABLE IF NOT EXISTS default.service_requests (
    case_summary            STRING,
    case_status             STRING,
    case_source             STRING,
    case_created_date       DATE,
    case_created_dttm       TIMESTAMP,
    case_closed_date        DATE,
    case_closed_dttm        TIMESTAMP,
    first_call_resolution   STRING,
    customer_zip_code       BIGINT,
    incident_address_1      STRING,
    incident_address_2      STRING,
    incident_intersection_1 STRING,
    incident_intersection_2 STRING,
    incident_zip_code       BIGINT,
    longitude               DOUBLE,
    latitude                DOUBLE,
    agency                  STRING,
    division                STRING,
    major_area              STRING,
    type                    STRING,
    topic                   STRING,
    council_district        STRING,
    police_district         STRING,
    neighborhood            STRING
)
PARTITIONED BY (day DATE)
STORED AS PARQUET
LOCATION 's3://nmcintyre-hive/tables/parquet/service_requests'
;

INSERT OVERWRITE TABLE default.service_requests
PARTITION (day)
SELECT
  TO_STRING(case_summary)             AS case_summary,
  TO_STRING(case_status)              AS case_status,
  TO_STRING(case_source)              AS case_source,
  TO_DATE(case_created_date)          AS case_created_date,
  TO_TIMESTAMP(case_created_dttm)     AS case_created_dttm,
  TO_DATE(case_closed_date)           AS case_closed_date,
  TO_TIMESTAMP(case_closed_dttm)      AS case_closed_dttm,
  TO_STRING(first_call_resolution)    AS first_call_resolution,
  TO_BIGINT(customer_zip_code)        AS customer_zip_code,
  TO_STRING(incident_address_1)       AS incident_address_1,
  TO_STRING(incident_address_2)       AS incident_address_2,
  TO_STRING(incident_intersection_1)  AS incident_intersection_1,
  TO_STRING(incident_intersection_2)  AS incident_intersection_2,
  TO_BIGINT(incident_zip_code)        AS incident_zip_code,
  TO_DOUBLE(longitude)                AS longitude,
  TO_DOUBLE(latitude)                 AS latitude,
  TO_STRING(agency)                   AS agency,
  TO_STRING(division)                 AS division,
  TO_STRING(major_area)               AS major_area,
  TO_STRING(type)                     AS type,
  TO_STRING(topic)                    AS topic,
  TO_STRING(council_district)         AS council_district,
  TO_STRING(police_district)          AS police_district,
  TO_STRING(neighborhood)             AS neighborhood,
  TO_DATE(case_created_date)          AS day
FROM
  raw.service_requests
;