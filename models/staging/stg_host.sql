with

source  as (

    select * from {{ ref('snapshot_host') }}

),

renamed as (
    select
        listing_id,
        scraped_date,
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        dbt_scd_id,
        dbt_updated_at,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as dbt_valid_from,
        dbt_valid_to
    from source
),

select * from renamed