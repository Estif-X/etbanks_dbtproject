SELECT
    sn AS serial_number,
    banks AS bank_name,
    CAST(REPLACE(REPLACE(eps, ',', ''), '%', '') AS DECIMAL(18, 2)) AS eps, -- Added REPLACE for '%'
    CAST(REPLACE(REPLACE(roa, ',', ''), '%', '') AS DECIMAL(18, 4)) AS roa, -- Added REPLACE for '%'
    CAST(REPLACE(REPLACE(roe, ',', ''), '%', '') AS DECIMAL(18, 4)) AS roe, -- Added REPLACE for '%'
    CAST(REPLACE(net_income, ',', '') AS DECIMAL(38, 2)) AS net_income,
    CAST(REPLACE(total_asset, ',', '') AS DECIMAL(38, 2)) AS total_asset,
    CAST(REPLACE(total_capital, ',', '') AS DECIMAL(38, 2)) AS total_capital,
    CAST(REPLACE(paid_up_capital, ',', '') AS DECIMAL(38, 2)) AS paid_up_capital,
    CAST(REPLACE(profit_before_tax, ',', '') AS DECIMAL(38, 2)) AS profit_before_tax,
    CAST(REPLACE(net_income_in_usd, ',', '') AS DECIMAL(38, 2)) AS net_income_in_usd,
    CAST(REPLACE(total_asset_in_usd, ',', '') AS DECIMAL(38, 2)) AS total_asset_in_usd,
    CAST(REPLACE(total_capital_in_usd, ',', '') AS DECIMAL(38, 2)) AS total_capital_in_usd,
    CAST(REPLACE(paid_up_capital_in_usd, ',', '') AS DECIMAL(38, 2)) AS paid_up_capital_in_usd,
    CAST(REPLACE(profit_for_the_year_000, ',', '') AS DECIMAL(38, 2)) AS profit_for_the_year_000,
    CAST(REPLACE(profit_before_tax_in_usd, ',', '') AS DECIMAL(38, 2)) AS profit_before_tax_in_usd,
    CAST(REPLACE(profit_for_the_year_in_usd, ',', '') AS DECIMAL(38, 2)) AS profit_for_the_year_in_usd
FROM
    {{ source('raw_data', 'banks_2022_2023_raw') }}