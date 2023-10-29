{{
    config(
        unique_key='property_type'
    )
}}

with

source  as (
    select * from {{ ref('property_snapshot') }}
),

renamed as (
    select
        property_type
    from source
)

select * from renamed