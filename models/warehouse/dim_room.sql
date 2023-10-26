{{
    config(
        unique_key='room_type'
    )
}}

select * from {{ ref('room_stg') }}