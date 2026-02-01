
  
    

create or replace transient table AIRBNB_PRO.gold.facts
    
    
    
    as (-- we can see that all the columns are being read from the gold_obt table.
-- we are not referring to any columns from the other two tables ie. dim_listings and dim_hosts
-- we are also not using dim_bookings table at all in this query.



-- select all the columns available from the first table in the congigs list
SELECT 
        GOLD_obt.BOOKING_ID, GOLD_obt.LISTING_ID, GOLD_obt.HOST_ID,GOLD_obt.total_fee_amount, GOLD_obt.ACCOMMODATES, GOLD_obt.BEDROOMS, GOLD_obt.BATHROOMS, GOLD_obt.PRICE_PER_NIGHT, GOLD_obt.RESPONSE_RATE

FROM
    
    
      AIRBNB_PRO.GOLD.OBT AS GOLD_obt
    
        
    
        LEFT JOIN AIRBNB_PRO.GOLD.DIM_LISTINGS AS DIM_listings
        ON GOLD_obt.listing_id = DIM_listings.listing_id
        
        
    
        LEFT JOIN AIRBNB_PRO.GOLD.DIM_HOSTS AS DIM_hosts
        ON GOLD_obt.host_id = DIM_hosts.host_id
        
        
    )
;


  