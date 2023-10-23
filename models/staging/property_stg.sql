{{
    config(
        unique_key='dbt_scd_id'
    )
}}

with

source  as (
    select * from {{ ref('property_snapshot') }}
),

renamed as (
    select
        DISTINCT property_type
    from source
)

select * from renamed