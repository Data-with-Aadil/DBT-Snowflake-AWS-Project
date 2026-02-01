{% set configs = [
    {
        "table" : "AIRBNB_PRO.SILVER.SILVER_BOOKINGS",
        "columns" : "SILVER_BOOKINGS.*",
        "alias" : "SILVER_bookings"
    },
    {
        "table" : "AIRBNB_PRO.SILVER.SILVER_LISTINGS",
        "columns" : "SILVER_listings.HOST_ID, SILVER_listings.PROPERTY_TYPE, SILVER_listings.ROOM_TYPE, SILVER_listings.CITY, SILVER_listings.COUNTRY, SILVER_listings.ACCOMMODATES, SILVER_listings.BEDROOMS, SILVER_listings.BATHROOMS, SILVER_listings.PRICE_PER_NIGHT, silver_listings.PRICE_PER_NIGHT_TAGGED, SILVER_listings.CREATED_AT AS LISTING_CREATED_AT",
        "alias" : "SILVER_listings",
        "join_condition" : "SILVER_bookings.listing_id = SILVER_listings.listing_id"
    },
    {
        "table" : "AIRBNB_PRO.SILVER.SILVER_HOSTS",
        "columns" : "SILVER_hosts.HOST_NAME, SILVER_hosts.HOST_SINCE, SILVER_hosts.IS_SUPERHOST, SILVER_hosts.RESPONSE_RATE, SILVER_hosts.RESPONSE_RATE_QUALITY, SILVER_hosts.CREATED_AT AS HOST_CREATED_AT",
        "alias" : "SILVER_hosts",
        "join_condition" : "SILVER_listings.HOST_ID = SILVER_hosts.HOST_ID"

    }
]%}

-- now once the configs are set, we can dynamically build the SQL query
-- for this , we need to loop through the configs and build the select and join statements
-- now, since our pipeline is a metadata driven pipeline, if any new table comes into the future, we just need to add a new config to the configs list above
-- first we will iterate trhough all the columns, and will add a if not loop.last to avoid trailing commas.

{# Using Jinja write the Join Query …
Here loop.last is to make sure, we don’t add a comma at last.
Loop.first is representing the “From” Clause.
Else… is representing the all other tables which would be joining.
We are just saying ki if there is only 1 table go ahead and just select the tables.. If another tables are present , then do the left join… #}
select 
    {% for config in configs %}
        {{ config["columns"] }}{% if not loop.last %},{% endif %}
    {% endfor %}
-- then we will use the from statement
FROM
    {% for config in configs %}
        {% if loop.first %}
            {{ config['table'] }} AS {{ config['alias'] }}
        {% else %}
            LEFT JOIN {{ config['table'] }} AS {{ config['alias'] }}
            ON {{ config['join_condition'] }}
        {% endif %}
    {% endfor %}

-- so basically we are running 2 for loops here.
-- first we are looping through the configs to get all the columns for the select statement
-- then we are looping through the configs again to build the from and join statements
-- if the table is the first table, we just add it to the from clause
-- else, if we have more tables, we add it as a left join with the join condition specified in the config