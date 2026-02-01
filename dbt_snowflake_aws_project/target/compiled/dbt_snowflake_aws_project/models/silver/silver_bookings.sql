

select 
    booking_id,
    listing_id,
    booking_date,
    booking_status,
    
    round(NIGHTS_BOOKED * BOOKING_AMOUNT,2)
 as total_booking_amount,
    CLEANING_FEE + SERVICE_FEE as total_fee_amount,
    CREATED_AT
from
    AIRBNB_PRO.semi_raw.bookings