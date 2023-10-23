{% snapshot listings_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='scraped_date',
        )
}}

  select 
    listing_id,
    scraped_date,
    listing_neighbourhood,
    price,
    has_availability,
    availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
  from {{ source('raw', 'listings') }}

{% endsnapshot %}

