{{
    config(
        unique_key='dbt_scd_id'
    )
}}

with

source  as (
    select * from {{ ref('room_snapshot') }}
),

renamed as (
    select
        room_type,
        accommodates
    from source
)

select * from renamed