SELECT
    *,
    2023 AS reporting_year -- Assign year for data from banks_2022_2023_raw
FROM
    {{ ref('stg_banks_2022_2023_raw') }}

UNION ALL

SELECT
    *,
    2024 AS reporting_year -- Assign year for data from banks_2023_2024_raw
FROM
    {{ ref('stg_banks_2023_2024_raw') }}