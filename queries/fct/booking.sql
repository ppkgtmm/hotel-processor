CREATE TEMPORARY TABLE fact_booking AS
    WITH raw_bookings AS (
        SELECT 
            br.id,
            b.checkin,
            b.checkout,
            br.room,
            br.guest,
            br.updated_at
        FROM staging.booking_room br
        JOIN staging.booking b
        ON br.booking = b.id
        WHERE br.is_deleted = false 
        AND br.date_processed IS NULL 
        AND b.is_deleted = false 
        AND DATE_DIFF(b.checkin, DATE_ADD(DATE(@run_time), INTERVAL 1 DAY), DAY) < 7
    ), bookings AS (
        SELECT
            b.id,
            b.checkin,
            b.checkout,
            b.updated_at,
            (
                SELECT MAX(id)
                FROM warehouse.dim_guest
                WHERE _id = b.guest AND effective_from <= b.updated_at
            ) AS guest,
            (
                SELECT g.location
                FROM staging.guest g
                WHERE g.id = b.guest AND g.updated_at <= b.updated_at
                ORDER BY g.updated_at DESC
                LIMIT 1
            ) AS guest_location,
            (
                SELECT roomtype
                FROM staging.room r
                WHERE r.id = b.room AND r.updated_at <= b.updated_at
                ORDER BY r.updated_at DESC
                LIMIT 1
            ) AS roomtype
        FROM raw_bookings b
    ), enriched_bookings AS (
        SELECT
            b.id,
            b.guest,
            (
                SELECT MAX(id)
                FROM warehouse.dim_location
                WHERE _id = b.guest_location AND effective_from <= b.updated_at
            ) AS guest_location,
            (
                SELECT MAX(id)
                FROM warehouse.dim_roomtype
                WHERE _id = b.roomtype AND effective_from <= b.updated_at
            ) AS roomtype,
            date
        FROM bookings b,
        UNNEST(GENERATE_DATE_ARRAY(b.checkin, b.checkout)) AS date
        WHERE b.guest IS NOT NULL AND b.guest_location IS NOT NULL AND b.roomtype IS NOT NULL
    )

SELECT 
    id,
    CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date,
    guest,
    guest_location,
    roomtype
FROM enriched_bookings
WHERE guest_location IS NOT NULL AND roomtype IS NOT NULL;

INSERT INTO warehouse.fct_booking (date, guest, guest_location, roomtype)
SELECT date, guest, guest_location, roomtype
FROM fact_booking;

MERGE INTO staging.booking_rooms br
USING (
    SELECT id
    FROM fact_bookings
    GROUP BY 1
) fb
ON br.id = fb.id
WHEN MATCHED THEN UPDATE SET date_processed = CURRENT_TIMESTAMP();
