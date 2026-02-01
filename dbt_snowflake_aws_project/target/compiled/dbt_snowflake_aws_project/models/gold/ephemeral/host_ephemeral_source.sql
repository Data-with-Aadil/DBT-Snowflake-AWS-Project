

with hosts as
    (
        SELECT
            host_id,
            host_name,
            host_since,
            is_superhost,
            response_rate_quality,
            HOST_CREATED_AT
        FROM
            AIRBNB_PRO.gold.obt
    )

select * from hosts