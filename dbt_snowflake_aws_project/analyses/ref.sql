-- testing the reference macro
select * from {{ ref('bookings') }}