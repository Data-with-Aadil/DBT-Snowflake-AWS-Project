
  
    

create or replace transient table AIRBNB_PRO.gold.obt
    
    
    
    as (

-- now once the configs are set, we can dynamically build the SQL query
-- for this , we need to loop through the configs and build the select and join statements
-- now, since our pipeline is a metadata driven pipeline, if any new table comes into the future, we just need to add a new config to the configs list above
-- first we will iterate trhough all the columns, and will add a if not loop.last to avoid trailing commas.


select 
    
        SILVER_BOOKINGS.*,
    
        SILVER_listings.HOST_ID, SILVER_listings.PROPERTY_TYPE, SILVER_listings.ROOM_TYPE, SILVER_listings.CITY, SILVER_listings.COUNTRY, SILVER_listings.ACCOMMODATES, SILVER_listings.BEDROOMS, SILVER_listings.BATHROOMS, SILVER_listings.PRICE_PER_NIGHT, silver_listings.PRICE_PER_NIGHT_TAGGED, SILVER_listings.CREATED_AT AS LISTING_CREATED_AT,
    
        SILVER_hosts.HOST_NAME, SILVER_hosts.HOST_SINCE, SILVER_hosts.IS_SUPERHOST, SILVER_hosts.RESPONSE_RATE, SILVER_hosts.RESPONSE_RATE_QUALITY, SILVER_hosts.CREATED_AT AS HOST_CREATED_AT
    
-- then we will use the from statement
FROM
    
        
            AIRBNB_PRO.SILVER.SILVER_BOOKINGS AS SILVER_bookings
        
    
        
            LEFT JOIN AIRBNB_PRO.SILVER.SILVER_LISTINGS AS SILVER_listings
            ON SILVER_bookings.listing_id = SILVER_listings.listing_id
        
    
        
            LEFT JOIN AIRBNB_PRO.SILVER.SILVER_HOSTS AS SILVER_hosts
            ON SILVER_listings.HOST_ID = SILVER_hosts.HOST_ID
        
    

-- so basically we are running 2 for loops here.
-- first we are looping through the configs to get all the columns for the select statement
-- then we are looping through the configs again to build the from and join statements
-- if the table is the first table, we just add it to the from clause
-- else, if we have more tables, we add it as a left join with the join condition specified in the config
    )
;


  