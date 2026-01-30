{{
    config (
        materialized = 'ephemeral'
    )
}}

with bookings as
    (
        select
            booking_id,
            booking_date,
            booking_status,
            total_fee_amount,
            CREATED_AT
        from
            {{ ref('obt') }}
    )

select * from bookings