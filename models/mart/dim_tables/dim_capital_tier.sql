SELECT
    capital_tier_sk AS capital_tier_id, -- Primary Key: Surrogate key generated in int_banks_enriched
    capital_tier
FROM
    {{ ref('int_banks_enriched') }}
GROUP BY
    capital_tier_sk,
    capital_tier
ORDER BY
    CASE
        WHEN capital_tier = 'Large Capital' THEN 1
        WHEN capital_tier = 'Medium Capital' THEN 2
        WHEN capital_tier = 'Small Capital' THEN 3
        ELSE 4
    END