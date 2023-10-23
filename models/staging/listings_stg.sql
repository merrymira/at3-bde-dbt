{{
    config(
        unique_key='dbt_scd_id'
    )
}}

with

source  as (
    select * from {{ ref('listings_snapshot') }}
),

renamed as (
    select
        listing_id,
        listing_neighbourhood,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        CASE WHEN review_scores_rating = 'NaN' THEN 0 ELSE review_scores_rating END as review_scores_rating,
        CASE WHEN review_scores_accuracy = 'NaN' THEN 0 ELSE review_scores_accuracy END as review_scores_accuracy,
        CASE WHEN review_scores_cleanliness = 'NaN' THEN 0 ELSE review_scores_cleanliness END as review_scores_cleanliness,
        CASE WHEN review_scores_checkin = 'NaN' THEN 0 ELSE review_scores_checkin END as review_scores_checkin,
        CASE WHEN review_scores_communication = 'NaN' THEN 0 ELSE review_scores_communication END as review_scores_communication,
        CASE WHEN review_scores_value = 'NaN' THEN 0 ELSE review_scores_value END as review_scores_value
    from source
)

select * from renamed