-----
-----
----- Seasonal trends
----- requests most frequent on weekdays for top 5 occurring events
-----
-----
SELECT case_summary, EXTRACT(DOW FROM case_created_dttm) AS dow, COUNT(*)
FROM service_requests
WHERE case_summary IN 
  (
    SELECT case_summary
    FROM
      (
        SELECT case_summary, DENSE_RANK() OVER (ORDER BY occurrences DESC) AS rank
        FROM
          (
            SELECT case_summary, COUNT(*) AS occurrences
            FROM service_requests
            WHERE case_summary IS NOT NULL
            GROUP BY case_summary
          ) events
      ) ranked_events
    WHERE rank <= 5
  )
GROUP BY case_summary, EXTRACT(DOW FROM case_created_dttm)
ORDER BY case_summary, dow

 case_summary | dow | count 
--------------+-----+-------
 DMV          |   0 |     5
 DMV          |   1 |  1508
 DMV          |   2 |  1271
 DMV          |   3 |  1346
 DMV          |   4 |  1277
 DMV          |   5 |  1350
 DMV          |   6 |    21
 EXTRA TRASH  |   0 |   130
 EXTRA TRASH  |   1 |  1124
 EXTRA TRASH  |   2 |  1183
 EXTRA TRASH  |   3 |  1070
 EXTRA TRASH  |   4 |   761
 EXTRA TRASH  |   5 |   599
 EXTRA TRASH  |   6 |   145
 GRAFFITI     |   0 |   385
 GRAFFITI     |   1 |  1326
 GRAFFITI     |   2 |  1226
 GRAFFITI     |   3 |  1272
 GRAFFITI     |   4 |  1120
 GRAFFITI     |   5 |   971
 GRAFFITI     |   6 |   516
 MISSED TRASH |   0 |    39
 MISSED TRASH |   1 |   548
 MISSED TRASH |   2 |   816
 MISSED TRASH |   3 |  1018
 MISSED TRASH |   4 |  1064
 MISSED TRASH |   5 |   937
 MISSED TRASH |   6 |   224
 REGISTRATION |   0 |     4
 REGISTRATION |   1 |  1139
 REGISTRATION |   2 |  1147
 REGISTRATION |   3 |  1101
 REGISTRATION |   4 |  1012
 REGISTRATION |   5 |  1033
 REGISTRATION |   6 |    45


-----
-----
----- Most common events by geography
-----
-----
SELECT
  top3.police_district,
  case_summary,
  occurrences,
  ROUND(((occurrences * 1.0) / events) * 100, 2) AS pct_occurrence,
  rank
FROM
  (
    SELECT police_district, case_summary, occurrences, rank
    FROM
      (
        SELECT police_district, case_summary, occurrences,
               DENSE_RANK() OVER (PARTITION BY police_district ORDER BY occurrences DESC) AS rank
        FROM
          (
            SELECT police_district, case_summary, COUNT(*) AS occurrences
            FROM service_requests
            WHERE police_district IS NOT NULL
            GROUP BY police_district, case_summary
          ) district_event_counts
      ) ranked_event_counts
    WHERE rank <= 3
      AND occurrences >= 2
    ORDER BY police_district, rank ASC
  ) top3_events
JOIN
  (
    SELECT police_district, COUNT(*) AS events
    FROM service_requests
    WHERE police_district IS NOT NULL
    GROUP BY police_district
  ) district_events
ON top3.police_district = district_events.police_district

  police_district |     case_summary      | occurrences | pct_occurrence | rank 
 -----------------+-----------------------+-------------+----------------+------
                1 | ILLEGAL DUMPING       |        1079 |           4.94 |    1
                1 | MISSED TRASH          |         932 |           4.27 |    2
                1 | GRAFFITI              |         781 |           3.57 |    3
                2 | GRAFFITI              |        1358 |           5.98 |    1
                2 | ILLEGAL DUMPING       |        1272 |           5.60 |    2
                2 | MISSED TRASH          |         916 |           4.03 |    3
                3 | MISSED TRASH          |        1083 |           3.70 |    1
                3 | POTHOLE               |         930 |           3.18 |    2
                3 | GRAFFITI              |         727 |           2.49 |    3
                4 | ILLEGAL DUMPING       |         974 |           4.98 |    1
                4 | MISSED TRASH          |         695 |           3.55 |    2
                4 | LOOSE DOG             |         540 |           2.76 |    3
                5 | MISSED TRASH          |         480 |           3.69 |    1
                5 | TRASH CART            |         365 |           2.81 |    2
                5 | LOOSE DOG             |         299 |           2.30 |    3
                6 | GRAFFITI              |        1860 |          22.08 |    1
                6 | POTHOLE               |         200 |           2.37 |    2
                6 | ILLEGAL DUMPING       |         168 |           1.99 |    3
                7 | FOOD POISONING        |           2 |           6.25 |    1
                7 | RESTAURANT INSPECTION |           2 |           6.25 |    1
                7 | POLICE ASSIST         |           2 |           6.25 |    1
                7 | INSPECTION            |           2 |           6.25 |    1


-----
-----
----- Typical response times
-----
-----
WITH response_times AS
  (
    SELECT police_district, case_summary,
           EXTRACT(EPOCH FROM case_closed_dttm) - EXTRACT(EPOCH FROM case_created_dttm) AS response_time
    FROM service_requests
    WHERE police_district IS NOT NULL
      AND case_summary IS NOT NULL
      AND case_closed_dttm IS NOT NULL
      AND case_created_dttm IS NOT NULL
  )
-- overall
SELECT AVG(response_time)
FROM response_times 
-- by geography and event 
SELECT police_district, case_summary, AVG(response_time)
FROM response_times
GROUP BY police_district, case_summary
-- by geogrpahy
SELECT police_district, AVG(response_time)
FROM response_times
GROUP BY police_district
-- by event
SELECT case_summary, AVG(response_time)
FROM response_times
GROUP BY case_summary
;


-----
-----
----- Correlations
-----
-----
SELECT
  accidents.district,
  CORR(requests.requests * 1.0, accidents.accidents * 1.0) AS correlation
FROM
  (
    SELECT
      district_id           AS district,
      YEAR(reported_date)   AS year,
      MONTH(reported_date)  AS month,
      COUNT(*)              AS accidents
    FROM
      traffic_accidents
    WHERE
      district_id IS NOT NULL
    GROUP BY
      district_id,
      YEAR(reported_date),
      MONTH(reported_date)
  ) accidents
JOIN
  (
    SELECT
      police_district                       AS district,
      EXTRACT(YEAR FROM case_created_date)  AS year,
      EXTRACT(MONTH FROM case_created_date) AS month,
      COUNT(*)                              AS requests
    FROM
      service_requests
    WHERE
       case_summary LIKE '%ACCIDENT%'
       OR case_summary LIKE '%TRAFFIC%'
    GROUP BY
      police_district,
      EXTRACT(YEAR FROM case_created_date),
      EXTRACT(MONTH FROM case_created_date)
  ) requests
ON
  accidents.district = requests.district
  AND accidents.year = requests.year
  AND accidents.month = requests.month
GROUP BY
  accidents.district
ORDER BY
  district

-----
----- no strong correlation between accident/traffic requests and traffic
----- accidents across districts
-----
  district |    correlation     
 ----------+--------------------
         1 |  0.459783168901975
         2 |  0.609151538865514
         3 |  0.579018083736484
         4 | -0.288125606333472
         5 |  0.148290849948706
         6 |  0.479089042447828

SELECT
 accidents.district,
 CORR(requests.requests * 1.0, accidents.accidents * 1.0) AS correlation
FROM
 (
   SELECT
     district_id           AS district,
     YEAR(reported_date)   AS year,
     MONTH(reported_date)  AS month,
     COUNT(*)              AS accidents
   FROM
     traffic_accidents
   WHERE
     district_id IS NOT NULL
   GROUP BY
     district_id,
     YEAR(reported_date),
     MONTH(reported_date)
 ) accidents
JOIN
 (
   SELECT
     police_district                       AS district,
     EXTRACT(YEAR FROM case_created_date)  AS year,
     EXTRACT(MONTH FROM case_created_date) AS month,
     COUNT(*)                              AS requests
   FROM
     service_requests
   WHERE
      case_summary LIKE '%ACCIDENT%'
   GROUP BY
     police_district,
     EXTRACT(YEAR FROM case_created_date),
     EXTRACT(MONTH FROM case_created_date)
 ) requests
ON
 accidents.district = requests.district
 AND accidents.year = requests.year
 AND accidents.month = requests.month
GROUP BY
 accidents.district
ORDER BY
 district

-----
----- strong correlation between "accident" requests and traffic accidents
----- in district 2
-----
 district |    correlation    
----------+-------------------
        1 |                  
        2 | 0.996038123932657
        3 |                  
        4 |                  
        5 |                  
        6 |                  
