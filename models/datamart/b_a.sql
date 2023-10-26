with source as (
    select * from {{ ref('listings_stg') }}
),

year_month as (
  select 
    to_date(scraped_date, 'YYYY-MM-DD') as year_month,
    *
  from source
),

active_listings as (
  select 
    extract(year from year_month) || '-' || LPAD(extract(month from year_month)::text, 2, '0') AS year_month,
    listing_neighbourhood,
    sum(case when has_availability = 't' then 1 else 0 end) as active_listings
  from year_month
  group by year_month, listing_neighbourhood
),

ranking_listings as (
  select 
    year_month,
    listing_neighbourhood,
    active_listings,
    RANK() OVER (PARTITION BY year_month ORDER BY active_listings DESC) AS rank
  FROM active_listings
)

select
    year_month, listing_neighbourhood,
    max(active_listings),
    min(active_listings),
    ROUND(avg(active_listings), 2)
from ranking_listings
group by listing_neighbourhood, year_month