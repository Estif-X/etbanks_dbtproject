WITH enriched_data AS (
    SELECT
        serial_number,
        bank_name,
        eps,
        roa,
        roe,
        net_income,
        total_asset,
        total_capital,
        paid_up_capital,
        profit_before_tax,
        net_income_in_usd,
        total_asset_in_usd,
        total_capital_in_usd,
        paid_up_capital_in_usd,
        profit_for_the_year_000,
        profit_before_tax_in_usd,
        profit_for_the_year_in_usd,
        reporting_year,
        -- Derive profit band based on net_income with more realistic thresholds
        CASE
            WHEN net_income >= 1000000000 THEN 'High Profit'
            WHEN net_income >= 500000000 AND net_income < 1000000000 THEN 'Medium-High Profit'
            WHEN net_income >= 100000000 AND net_income < 500000000 THEN 'Medium Profit'
            WHEN net_income > 0 THEN 'Low Profit'
            ELSE 'Loss'
        END AS profit_band,
        -- Derive capital tier based on total_capital with more realistic thresholds
        CASE
            WHEN total_capital >= 10000000000 THEN 'Large Capital' -- >= 10 Billion
            WHEN total_capital >= 1000000000 AND total_capital < 10000000000 THEN 'Medium Capital' -- >= 1 Billion
            ELSE 'Small Capital' -- < 1 Billion
        END AS capital_tier,
        -- Derive asset size based on total_asset with more realistic thresholds
        CASE
            WHEN total_asset >= 100000000000 THEN 'Large Asset Bank' -- >= 100 Billion
            WHEN total_asset >= 10000000000 AND total_asset < 100000000000 THEN 'Medium Asset Bank' -- >= 10 Billion
            ELSE 'Small Asset Bank' -- < 10 Billion
        END AS asset_size
    FROM
        {{ ref('int_banks_raw') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['bank_name']) }} AS bank_sk,
    {{ dbt_utils.generate_surrogate_key(['reporting_year']) }} AS year_sk,
    {{ dbt_utils.generate_surrogate_key(['profit_band']) }} AS profit_band_sk,
    {{ dbt_utils.generate_surrogate_key(['capital_tier']) }} AS capital_tier_sk,
    {{ dbt_utils.generate_surrogate_key(['asset_size']) }} AS asset_size_sk,
    serial_number,
    bank_name,
    eps,
    roa,
    roe,
    net_income,
    total_asset,
    total_capital,
    paid_up_capital,
    profit_before_tax,
    net_income_in_usd,
    total_asset_in_usd,
    total_capital_in_usd,
    paid_up_capital_in_usd,
    profit_for_the_year_000,
    profit_before_tax_in_usd,
    profit_for_the_year_in_usd,
    reporting_year,
    profit_band,
    capital_tier,
    asset_size
FROM
    enriched_data