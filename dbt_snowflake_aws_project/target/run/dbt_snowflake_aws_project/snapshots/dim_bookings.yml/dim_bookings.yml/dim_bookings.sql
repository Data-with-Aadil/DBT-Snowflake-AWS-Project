
      
  
    

create or replace transient table AIRBNB_PRO.gold.dim_bookings
    
    
    
    as (
    

    select *,
        md5(coalesce(cast(booking_id as varchar ), '')
         || '|' || coalesce(cast(created_at as varchar ), '')
        ) as dbt_scd_id,
        created_at as dbt_updated_at,
        created_at as dbt_valid_from,
        
  
  coalesce(nullif(created_at, created_at), to_date('9999-12-31'))
  as dbt_valid_to
from (
        with __dbt__cte__booking_ephemeral_source as (


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
) select * from __dbt__cte__booking_ephemeral_source
    ) sbq



    )
;


  
  