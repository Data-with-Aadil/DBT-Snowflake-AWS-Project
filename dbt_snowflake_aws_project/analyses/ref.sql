-- testing the reference macro

{% set nights_booked = 1%}

select * from {{ ref('bookings') }} where nights_booked > {{ nights_booked }}