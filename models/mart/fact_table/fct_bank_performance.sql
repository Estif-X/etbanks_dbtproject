SELECT
    serial_number, -- Primary Key for the fact record
    int_enriched.bank_sk AS bank_id, -- Foreign Key to dim_bank
    int_enriched.year_sk AS year_id, -- Foreign Key to dim_date
    int_enriched.profit_band_sk AS profit_band_id, -- Foreign Key to dim_profit_band
    int_enriched.capital_tier_sk AS capital_tier_id, -- Foreign Key to dim_capital_tier
    int_enriched.asset_size_sk AS asset_size_id, -- Foreign Key to dim_asset_size
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
    profit_for_the_year_in_usd
FROM
    {{ ref('int_banks_enriched') }} int_enriched