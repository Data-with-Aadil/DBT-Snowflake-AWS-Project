
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

SELECT 
    1
FROM 
    AIRBNB_PRO.STAGING.bookings
WHERE 
    BOOKING_AMOUNT < 200
  
  
      
    ) dbt_internal_test