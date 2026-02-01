{% set cols = ['NIGHTS_BOOKED', 'BOOKING_ID', 'BOOKING_AMOUNT'] %}

-- iterate throught the list of cols
-- print the columns(select) ==> {col}
Select
{% for col in cols%}
    {{ col }}
    {% if not loop.last%}, {% endif %}
{% endfor %}
from {{ ref('bookings')}}