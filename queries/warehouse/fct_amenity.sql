CREATE TEMPORARY TABLE fact_amenities AS
    WITH raw_amenities AS (
        SELECT
            ba.id,
            ba.datetime,
            ba.addon,
            ba.quantity,
            br.guest,
            br.room,
            a.updated_at
        FROM staging.booking_addon ba JOIN staging.booking_room br ON ba.booking_room = br.id
        JOIN staging.booking b ON br.booking = b.id
        WHERE ba.is_deleted = false
        AND ba.date_processed IS NULL
        AND br.is_deleted = false
        AND b.is_deleted = false
        AND DATE_DIFF(b.checkin, DATE_ADD(DATE(@run_time), INTERVAL 1 DAY), DAY) < 7
    ),  dim_guest AS (
        SELECT a.id AS booking_addon, MAX(g.id) AS id
        FROM raw_amenities a JOIN warehouse.dim_guest g ON g._id = a.guest AND g.effective_from <= a.updated_at
        GROUP BY booking_addon
    ), guest_location AS (
        SELECT a.id AS booking_addon, a.updated_at, ARRAY_AGG(g.location ORDER BY g.updated_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS location
        FROM raw_amenities a JOIN staging.guest g ON g.id = a.guest AND g.updated_at <= a.updated_at
        GROUP BY booking_addon, a.updated_at
    ), dim_location AS (
        SELECT booking_addon, MAX(l.id) AS id
        FROM guest_location a JOIN warehouse.dim_location l ON l._id = a.location AND l.effective_from <= a.updated_at
        GROUP BY booking_addon
    ), roomtype AS (
        SELECT a.id AS booking_addon, a.updated_at, ARRAY_AGG(r.roomtype ORDER BY r.updated_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS roomtype
        FROM raw_amenities a JOIN staging.room r ON r.id = a.room AND r.updated_at <= a.updated_at
        GROUP BY booking_addon, a.updated_at
    ), dim_roomtype AS (
        SELECT booking_addon, MAX(t.id) AS id
        FROM roomtype a JOIN warehouse.dim_roomtype t ON t._id = a.roomtype AND t.effective_from <= a.updated_at
        GROUP BY booking_addon
    ),  dim_addon AS (
        SELECT a.id AS booking_addon, MAX(da.id) AS id
        FROM raw_amenities a JOIN warehouse.dim_addon da ON da._id = a.addon AND da.effective_from <= a.updated_at
        GROUP BY booking_addon
    ), enriched_amenities AS (
        SELECT
            a.id,
            a.datetime,
            g.id AS guest,
            l.id AS guest_location,
            t.id AS roomtype,
            ad.id AS addon,
            a.quantity
        FROM raw_amenities a JOIN dim_guest g ON a.id = g.booking_addon
        JOIN dim_location l ON a.id = l.booking_addon
        JOIN dim_roomtype t ON a.id = t.booking_addon
        JOIN dim_addon ad ON a.id = ad.booking_addon
    )
SELECT
    id,
    CAST(FORMAT_TIMESTAMP('%Y%m%d', datetime) AS INT64) AS date,
    CAST(FORMAT_TIMESTAMP('%H%M%S', datetime) AS INT64) AS time,
    guest,
    guest_location,
    roomtype,
    addon,
    quantity AS addon_quantity
FROM enriched_amenities;

INSERT INTO warehouse.fct_amenity (date, time, guest, guest_location, roomtype, addon, addon_quantity)
SELECT date, time, guest, guest_location, roomtype, addon, addon_quantity
FROM fact_amenities;

MERGE INTO staging.booking_addon ba
USING (
    SELECT id
    FROM fact_amenities
    GROUP BY 1
) fa
ON ba.id = fa.id
WHEN MATCHED THEN UPDATE SET date_processed = CURRENT_TIMESTAMP();
