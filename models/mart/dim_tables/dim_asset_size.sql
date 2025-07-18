SELECT
    asset_size_sk AS asset_size_id, -- Primary Key: Surrogate key generated in int_banks_enriched
    asset_size
FROM
    {{ ref('int_banks_enriched') }}
GROUP BY
    asset_size_sk,
    asset_size
ORDER BY
    CASE
        WHEN asset_size = 'Large Asset Bank' THEN 1
        WHEN asset_size = 'Medium Asset Bank' THEN 2
        WHEN asset_size = 'Small Asset Bank' THEN 3
        ELSE 4
    END