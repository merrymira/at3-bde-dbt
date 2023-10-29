{{
    config(
        unique_key='host_id'
    )
}}

with

source  as (
    select * from {{ ref('host_snapshot') }}
),

renamed AS (
    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        CASE WHEN host_neighbourhood = 'NaN' THEN 'Unknown' ELSE host_neighbourhood END AS host_neighbourhood
    FROM source
    where not (
        host_name = 'NaN' 
        and host_since = 'NaN' 
        and host_is_superhost = 'NaN' 
    )
)

select * from renamed



