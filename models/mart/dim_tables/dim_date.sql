SELECT
    year_sk AS year_id, -- Primary Key: Surrogate key generated in int_banks_enriched
    reporting_year
FROM
    {{ ref('int_banks_enriched') }}
GROUP BY
    year_sk,
    reporting_year
ORDER BY
    reporting_year