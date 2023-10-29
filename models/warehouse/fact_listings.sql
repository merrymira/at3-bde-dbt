SELECT 
    ls.listing_id,
    ls.scraped_date,
    ls.host_id,
    ls.host_name,
    ls.host_since,
    ls.host_is_superhost,
    ls.host_neighbourhood,
    CASE
        WHEN ls.host_neighbourhood = 'Unknown' THEN 'Unknown'
        ELSE dl.lga_name
    END AS host_neighbourhood_lga,
    ls.listing_neighbourhood,
    dl.lga_code,
    ls.property_type,
    ls.room_type,
    ls.accommodates,
    ls.price,
    ls.has_availability,
    ls.availability_30,
    ls.number_of_reviews,
    ls.review_scores_rating,
    ls.review_scores_accuracy,
    ls.review_scores_cleanliness,
    ls.review_scores_checkin,
    ls.review_scores_communication,
    ls.review_scores_value
FROM {{ ref('listings_stg') }} AS ls
left join {{ ref('dim_lga') }} as dl on ls.listing_neighbourhood = dl.lga_name

