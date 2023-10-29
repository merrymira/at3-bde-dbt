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
        property_type,
        room_type,
        accommodates,
        price,
        (30 - availability_30) AS number_of_stays
    FROM basegroup
    GROUP BY listing_neighbourhood, year_month, price, number_of_stays, property_type, room_type, accommodates, number_of_stays
),
estimated_revenue AS (
    SELECT
        listing_neighbourhood,
        year_month,
        property_type,
        room_type,
        accommodates,
        number_of_stays,
        round((sum(number_of_stays * price)), 2) AS estimated_revenue
    FROM active_listings
    GROUP BY listing_neighbourhood, year_month, property_type, room_type, accommodates, number_of_stays
),
top_neighborhoods AS (
    SELECT
        listing_neighbourhood
    FROM estimated_revenue
    GROUP BY listing_neighbourhood
    ORDER BY SUM(estimated_revenue) DESC
    LIMIT 5
),
best_listing_combinations AS (
    SELECT
        tn.listing_neighbourhood,
        er.property_type,
        er.room_type,
        er.accommodates,
        SUM(er.number_of_stays) AS total_stays
    FROM estimated_revenue er
    JOIN top_neighborhoods tn ON er.listing_neighbourhood = tn.listing_neighbourhood
    GROUP BY tn.listing_neighbourhood, er.property_type, er.room_type, er.accommodates
    ORDER BY SUM(er.number_of_stays) DESC
    LIMIT 5
)
SELECT
    best_listing_combinations.listing_neighbourhood,
    best_listing_combinations.property_type,
    best_listing_combinations.room_type,
    best_listing_combinations.accommodates,
    best_listing_combinations.total_stays
FROM best_listing_combinations
