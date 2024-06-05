
DELETE 
FROM staging.booking_addon
WHERE is_deleted = true OR date_processed IS NOT NULL;

MERGE INTO staging.booking_room t
USING (
    SELECT booking_room
    FROM staging.booking_addon
    WHERE date_processed IS NULL AND is_deleted = false
    GROUP BY booking_room
) s
ON t.id = s.booking_room
WHEN NOT MATCHED BY SOURCE AND (t.date_processed IS NOT NULL OR t.is_deleted = true) THEN DELETE;

MERGE INTO staging.booking t
USING (
    SELECT booking
    FROM staging.booking_room
    WHERE date_processed IS NULL AND is_deleted = false
    GROUP BY booking
) s
ON t.id = s.booking
WHEN NOT MATCHED BY SOURCE THEN DELETE;

DELETE FROM staging.booking WHERE is_deleted = true;

DELETE FROM staging.guest t
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT id, updated_at
    FROM staging.guest
    QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 3
  ) s
  WHERE t.id = s.id AND t.updated_at < s.updated_at 
);

DELETE FROM staging.room t
WHERE EXISTS (
  SELECT 1
  FROM (
    SELECT id, updated_at
    FROM staging.room
    QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_at DESC) = 3
  ) s
  WHERE t.id = s.id AND t.updated_at < s.updated_at 
);
