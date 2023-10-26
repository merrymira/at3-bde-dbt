{{
    config(
        unique_key='room_type'
    )
}}

with

source  as (
    select * from {{ ref('room_snapshot') }}
),

renamed as (
    select
        distinct room_type 
    from source
)

select * from renamed