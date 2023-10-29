-- Step 1: Identify Hosts with Multiple Listings
WITH host_listing_count AS (
    SELECT
        host_id,
        COUNT(*) AS listing_count
    FROM {{ ref('fact_listings') }}
    GROUP BY host_id
    HAVING COUNT(*) > 1
),
-- Step 2: Determine Host's Primary Residence LGA (simplified assumption)
host_primary_lga AS (
    SELECT
        host_id,
        MAX(host_neighbourhood_lga) AS primary_lga -- Assuming 'lga' is the column for the host's primary residence LGA
    FROM {{ ref('fact_listings') }}
    GROUP BY host_id
),
-- Step 3: Compare Host's Primary Residence LGA with Listing LGAs
host_listing_lga AS (
    SELECT
        hl.host_id,
        hl.primary_lga AS primary_residence_lga,
        l.listing_neighbourhood AS listing_lga
    FROM host_primary_lga hl
    JOIN {{ ref('fact_listings') }} l ON hl.host_id = l.host_id
)
-- Step 4: Calculate the Percentage of Hosts with Listings in the Same LGA
SELECT
    COUNT(DISTINCT hll.host_id) AS hosts_with_same_lga_listings,
    COUNT(DISTINCT hl.host_id) AS total_hosts_with_multiple_listings,
    (COUNT(DISTINCT hll.host_id)::float / COUNT(DISTINCT hl.host_id)::float) * 100 AS percentage
FROM host_listing_lga hll
JOIN host_listing_count hl ON hll.host_id = hl.host_id
