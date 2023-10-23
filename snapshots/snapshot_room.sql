{% snapshot snapshot_room %}

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
    room_type,
    accommodates
  from {{ source('raw', 'listings') }}

{% endsnapshot %}
