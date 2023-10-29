WITH basegroup AS (
    SELECT 
        to_date(scraped_date, 'YYYY-MM') AS year_month,
        *
    FROM {{ ref('fact_listings') }}
),
active_listings AS (
    SELECT 
        extract(year from year_month) || '-' || LPAD(extract(month from year_month)::text, 2, '0') AS year_month,
        listing_neighbourhood,
        lga_code,
        price,
        (30 - availability_30) AS number_of_stays
    FROM basegroup
    GROUP BY listing_neighbourhood, year_month, price, number_of_stays, lga_code
),
estimated_revenue AS (
    SELECT 
        listing_neighbourhood,
        lga_code,
        year_month,
        round((sum(number_of_stays * price)), 2) AS estimated_revenue
    FROM active_listings
    GROUP BY listing_neighbourhood, year_month, lga_code
),
best_worst_neighborhoods AS (
    SELECT 
        listing_neighbourhood,
        lga_code,
        MAX(estimated_revenue) AS max_revenue,
        MIN(estimated_revenue) AS min_revenue
    FROM estimated_revenue
    GROUP BY listing_neighbourhood, lga_code
),
best_performing_neighborhood AS (
    SELECT 
        bwn.lga_code AS best_performing_neighborhood,
        SUM(
            CASE 
                WHEN er.lga_code = bwn.lga_code 
                THEN age_0_4_yr_p + age_5_14_yr_p + age_15_19_yr_p + age_20_24_yr_p + age_25_34_yr_p
                ELSE 0
            END
        ) AS total_population_under_30_best
    FROM {{ ref('dim_g01') }} bwn
    LEFT JOIN estimated_revenue er ON bwn.lga_code = er.lga_code
    WHERE er.estimated_revenue = (SELECT MAX(max_revenue) FROM best_worst_neighborhoods)
    GROUP BY bwn.lga_code
),
worst_performing_neighborhood AS (
    SELECT 
        bwn2.lga_code AS worst_performing_neighborhood,
        SUM(
            CASE 
                WHEN er2.lga_code = bwn2.lga_code 
                THEN age_0_4_yr_p + age_5_14_yr_p + age_15_19_yr_p + age_20_24_yr_p + age_25_34_yr_p
                ELSE 0
            END
        ) AS total_population_under_30_worst
    FROM {{ ref('dim_g01') }} bwn2
    LEFT JOIN estimated_revenue er2 ON bwn2.lga_code = er2.lga_code
    WHERE er2.estimated_revenue = (SELECT MIN(min_revenue) FROM best_worst_neighborhoods)
    GROUP BY bwn2.lga_code
)
SELECT 
    best_performing_neighborhood,
    total_population_under_30_best,
    worst_performing_neighborhood,
    total_population_under_30_worst
FROM best_performing_neighborhood
CROSS JOIN worst_performing_neighborhood