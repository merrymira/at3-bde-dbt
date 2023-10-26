{{
    config(
        unique_key='lga_code'
    )
}}

select 
    lga_stg.lga_code as lga_code,
    lga_stg.lga_name as lga_name,
    suburb_stg.suburb_name as suburb_name
from {{ ref ('lga_stg') }} lga_stg
left join {{ ref ('suburb_stg') }} suburb_stg on lga_stg.lga_name = suburb_stg.lga_name

