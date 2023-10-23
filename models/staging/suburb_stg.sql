{{
    config(
        unique_key='lga_name'
    )
}}

with

source  as (
    select * from {{ source('raw', 'nsw_lga_suburb') }}
),

renamed as (
    select
        lga_name,
        suburb_name
    from source
)

select * from renamed