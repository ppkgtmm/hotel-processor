CREATE TEMPORARY TABLE fact_booking AS
    WITH raw_bookings AS (
        SELECT 
            br.id,
            b.checkin,
            b.checkout,
            br.room,
            br.guest,
            br.updated_at
        FROM staging.booking_room br JOIN staging.booking b ON br.booking = b.id
        WHERE br.is_deleted = false 
        AND br.date_processed IS NULL 
        AND b.is_deleted = false 
        AND DATE_DIFF(b.checkin, DATE_ADD(DATE(@run_time), INTERVAL 1 DAY), DAY) < 7
    ), dim_guest AS (
        SELECT b.id AS booking, MAX(g.id) AS id
        FROM raw_bookings b JOIN warehouse.dim_guest g ON g._id = b.guest AND g.effective_from <= b.updated_at
        GROUP BY booking
    ), guest_location AS (
        SELECT b.id AS booking, b.updated_at, ARRAY_AGG(g.location ORDER BY g.updated_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS location
        FROM raw_bookings b JOIN staging.guest g ON g.id = b.guest AND g.updated_at <= b.updated_at
        GROUP BY booking, b.updated_at
    ), dim_location AS (
        SELECT booking, MAX(l.id) AS id
        FROM guest_location b JOIN warehouse.dim_location l ON l._id = b.location AND l.effective_from <= b.updated_at
        GROUP BY booking
    ), roomtype AS (
        SELECT b.id AS booking, b.updated_at, ARRAY_AGG(r.roomtype ORDER BY r.updated_at DESC LIMIT 1)[SAFE_OFFSET(0)] AS roomtype
        FROM raw_bookings b JOIN staging.room r ON r.id = b.room AND r.updated_at <= b.updated_at
        GROUP BY booking, b.updated_at
    ), dim_roomtype AS (
        SELECT booking, MAX(t.id) AS id
        FROM roomtype b JOIN warehouse.dim_roomtype t ON t._id = b.roomtype AND t.effective_from <= b.updated_at
        GROUP BY booking
    ), bookings AS (
        SELECT
            b.id,
            b.checkin,
            b.checkout,
            b.updated_at,
            g.id AS guest,
            l.id AS guest_location,
            t.id AS roomtype
        FROM raw_bookings b JOIN dim_guest g ON b.id = g.booking
        JOIN dim_location l ON b.id = l.booking
        JOIN dim_roomtype t ON b.id = t.booking
    ), enriched_bookings AS (
        SELECT
            b.* EXCEPT(checkin, checkout, updated_at),
            date
        FROM bookings b,
        UNNEST(GENERATE_DATE_ARRAY(b.checkin, b.checkout)) AS date
    )

SELECT 
    id,
    CAST(FORMAT_DATE('%Y%m%d', date) AS INT64) AS date,
    guest,
    guest_location,
    roomtype
FROM enriched_bookings;

INSERT INTO warehouse.fct_booking (date, guest, guest_location, roomtype)
SELECT date, guest, guest_location, roomtype
FROM fact_booking;

MERGE INTO staging.booking_room br
USING (
    SELECT id
    FROM fact_bookings
    GROUP BY 1
) fb
ON br.id = fb.id
WHEN MATCHED THEN UPDATE SET date_processed = CURRENT_TIMESTAMP();
