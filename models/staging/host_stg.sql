{{
    config(
        unique_key='dbt_scd_id'
    )
}}

with

source  as (
    select * from {{ ref('host_snapshot') }}
),

renamed as (
    select
        DISTINCT listing_id,
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood
    from source
    where not (
        host_name = 'NaN'
        and host_since = 'NaN'
        and host_is_superhost = 'NaN'
        and host_neighbourhood = 'NaN'
    )
)

select * from renamed