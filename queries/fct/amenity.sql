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
    ), amenities AS (
        SELECT
            a.id,
            a.datetime,
            (
                SELECT MAX(id)
                FROM warehouse.dim_guest
                WHERE _id = a.guest AND effective_from <= a.updated_at
            ) AS guest,
            (
                SELECT g.location
                FROM staging.guest g
                WHERE g.id = a.guest AND g.updated_at <= a.updated_at
                ORDER BY g.updated_at DESC
                LIMIT 1
            ) AS guest_location,
            (
                SELECT roomtype
                FROM staging.room r
                WHERE r.id = a.room AND r.updated_at <= a.updated_at
                ORDER BY r.updated_at DESC
                LIMIT 1
            ) AS roomtype,
            (
                SELECT MAX(id)
                FROM warehouse.dim_addon
                WHERE _id = a.addon AND effective_from <= a.updated_at
            ) AS addon,
            a.quantity
        FROM raw_amenities a
    ), enriched_amenities AS (
        SELECT
            a.id,
            a.datetime,
            a.guest,
            a.addon,
            a.quantity,
            (
                SELECT MAX(id)
                FROM warehouse.dim_location
                WHERE _id = a.guest_location AND effective_from <= a.updated_at
            ) AS guest_location,
            (
                SELECT MAX(id)
                FROM warehouse.dim_roomtype
                WHERE _id = a.roomtype AND effective_from <= a.updated_at
            ) AS roomtype
        FROM amenities a
        WHERE a.guest IS NOT NULL AND a.guest_location IS NOT NULL AND a.roomtype IS NOT NULL AND a.addon IS NOT NULL
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
FROM enriched_amenities
WHERE guest_location IS NOT NULL AND roomtype IS NOT NULL;

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
