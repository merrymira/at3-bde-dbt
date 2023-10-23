{% snapshot snapshot_listings %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='scraped_date',
        )
    }}

  select * from {{ source('raw', 'listings') }}

{% endsnapshot %}