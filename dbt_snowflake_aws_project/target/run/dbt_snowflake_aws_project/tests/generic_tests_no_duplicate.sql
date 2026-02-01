
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

SELECT 
    BOOKING_ID, 
    COUNT(*)
FROM AIRBNB_PRO.STAGING.bookings
GROUP BY 1
HAVING COUNT(*) > 1
  
  
      
    ) dbt_internal_test