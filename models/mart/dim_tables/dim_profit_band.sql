SELECT
    profit_band_sk AS profit_band_id, -- Primary Key: Surrogate key generated in int_banks_enriched
    profit_band
FROM
    {{ ref('int_banks_enriched') }}
GROUP BY
    profit_band_sk,
    profit_band
ORDER BY
    CASE
        WHEN profit_band = 'High Profit' THEN 1
        WHEN profit_band = 'Medium-High Profit' THEN 2
        WHEN profit_band = 'Medium Profit' THEN 3
        WHEN profit_band = 'Low Profit' THEN 4
        WHEN profit_band = 'Loss' THEN 5
        ELSE 6
    END