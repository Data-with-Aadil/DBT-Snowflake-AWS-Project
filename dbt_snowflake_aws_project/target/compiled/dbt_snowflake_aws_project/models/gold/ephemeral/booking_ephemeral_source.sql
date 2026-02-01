

with bookings as
    (
        select
            booking_id,
            booking_date,
            booking_status,
            total_fee_amount,
            CREATED_AT
        from
            AIRBNB_PRO.gold.obt
    )

select * from bookings