{% snapshot room_snapshot %}

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
    room_type
  from {{ source('raw', 'listings') }}
  group by listing_id, scraped_date, room_type

{% endsnapshot %}
