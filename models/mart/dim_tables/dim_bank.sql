SELECT
    bank_sk AS bank_id, -- Primary Key: Surrogate key generated in int_banks_enriched
    bank_name
FROM
    {{ ref('int_banks_enriched') }}
GROUP BY
    bank_sk,
    bank_name