with basegroup as (
  select 
    to_date(scraped_date, 'YYYY-MM-DD') as year_month,
    *
  from {{ ref('fact_listings') }}
),

active_listings as (
  select 
    extract(year from year_month) || '-' || LPAD(extract(month from year_month)::text, 2, '0') AS year_month,
    property_type,
    room_type,
    accommodates,
    count(host_id) as total_hosts,
    count(listing_id) as total_listings,
    price,
    sum(case when has_availability = 't' then 1 else 0 end) as active_listings,
    (30-availability_30) as number_of_stays,
    avg(review_scores_rating) as review_scores_rating
  from basegroup
  group by property_type, room_type, accommodates, year_month, price, number_of_stays
)

select
    property_type,
    room_type,
    accommodates,
    year_month, 
    sum(active_listings / total_listings * 100) as active_listing_rate,
    min(price) min_price,
    max(price) max_price,
    percentile_cont(0.5) within group (order by price) AS median_price,
    round(avg(case when active_listings = 1 and price > 0 then price else 0 end), 2) avg_price,
    count(distinct total_hosts) as distinct_hosts,
    review_scores_rating as avg_review_scores_rating,
    sum(number_of_stays) as number_of_stays,
    avg(number_of_stays * price) as avg_estimated_revenue
from active_listings
group by property_type, room_type, accommodates, year_month, price, review_scores_rating