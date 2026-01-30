{{
    config(
        materialized = 'ephemeral'
    )
}}

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
            {{ ref('obt') }}
    )

select * from hosts