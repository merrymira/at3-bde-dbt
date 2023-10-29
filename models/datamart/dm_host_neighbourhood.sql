with basegroup as (
  select 
    to_date(scraped_date, 'YYYY-MM-DD') as year_month,
    *
  from {{ ref('fact_listings') }}
),

active_listings as (
  select 
    extract(year from year_month) || '-' || LPAD(extract(month from year_month)::text, 2, '0') AS year_month,
    host_neighbourhood_lga as lga_neighbourhood,
    count(host_id) as total_hosts,
    price,
    (30-availability_30) as number_of_stays
  from basegroup
  group by lga_neighbourhood, year_month, price, number_of_stays
)

select
    lga_neighbourhood,
    year_month,
    count(distinct total_hosts) as distinct_hosts,
    round((avg(number_of_stays * price)), 2) as estimated_revenue,
    round((avg(number_of_stays * price) / count(distinct total_hosts)), 2) as avg_per_host
from active_listings
group by lga_neighbourhood, year_month
