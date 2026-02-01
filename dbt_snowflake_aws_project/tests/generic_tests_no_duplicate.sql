{{ config(
    severity='warn') }}

SELECT 
    BOOKING_ID, 
    COUNT(*)
FROM {{ source('STAGING', 'bookings') }}
GROUP BY 1
HAVING COUNT(*) > 1