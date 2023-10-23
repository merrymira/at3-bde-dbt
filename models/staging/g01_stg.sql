{{
    config(
        unique_key='lga_code_2016'
    )
}}

with

source  as (
    select 
    NULLIF(regexp_replace(lga_code_2016, '\D','','g'), '')::numeric AS lga_code,
    * from {{ source('raw', 'census_g01') }}
),

renamed as (
    select *
    from source
)

select * from renamed