{{
    config(
        unique_key='property_type'
    )
}}

select * from {{ ref('property_stg') }}