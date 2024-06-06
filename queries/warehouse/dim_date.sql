MERGE INTO warehouse.dim_date t
USING (
    SELECT 
    CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS id,
    date, 
    EXTRACT(MONTH FROM date) AS month, 
    EXTRACT(QUARTER FROM date) AS quarter, 
    EXTRACT(YEAR FROM date) AS year
    FROM UNNEST(GENERATE_DATE_ARRAY((SELECT start_date FROM (
      SELECT COALESCE(
        (SELECT MAX(date) FROM warehouse.dim_date),
        (SELECT MIN(checkin) FROM staging.booking WHERE is_deleted = false)
    ) AS start_date
    )), DATE(@run_time))) AS date
) s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT ROW;
