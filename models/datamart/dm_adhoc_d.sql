WITH unique_listing_hosts AS (
    SELECT
        host_id,
        COUNT(*) AS listing_count,
        listing_neighbourhood,
        (30-availability_30) as number_of_stays,
        price
    FROM {{ ref('fact_listings') }}
    GROUP BY host_id, listing_neighbourhood, (30-availability_30), price
    HAVING COUNT(*) = 1
),
estimated_revenue AS (
    SELECT
        host_id,
        listing_neighbourhood,
        round((sum(number_of_stays * price)), 2) AS estimated_revenue
    FROM unique_listing_hosts
    GROUP BY host_id, listing_neighbourhood
),
annual_estimated_revenue AS (
    SELECT
        host_id,
        listing_neighbourhood,
        SUM(estimated_revenue) AS annual_revenue
    FROM estimated_revenue
    WHERE listing_neighbourhood IN (
        SELECT listing_neighbourhood
        FROM unique_listing_hosts
    )
    GROUP BY host_id, listing_neighbourhood
),
annual_median_mortgage AS (
    SELECT
        listing_neighbourhood,
        sum(median_price) as median_price-- Replace with the actual column name for median mortgage
    FROM {{ ref('dm_listing_neighbourhood') }}
    GROUP BY listing_neighbourhood
)
SELECT
    aeh.host_id,
    CASE
        WHEN aeh.annual_revenue >= amm.median_price THEN 'Yes'
        ELSE 'No'
    END AS can_cover_mortgage
FROM annual_estimated_revenue aeh
JOIN annual_median_mortgage amm ON aeh.listing_neighbourhood = amm.listing_neighbourhood