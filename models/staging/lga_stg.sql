{{
    config(
        unique_key='lga_code'
    )
}}

with

source  as (
    select * from {{ source('raw', 'nsw_lga_code') }}
),

renamed as (
    select
        lga_code,
        lga_name
    from source
)

select * from renamed