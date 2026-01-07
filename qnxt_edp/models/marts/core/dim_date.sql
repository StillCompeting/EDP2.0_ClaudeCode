{{
    config(
        materialized='table',
        tags=['silver', 'core', 'dimension']
    )
}}

{#
    Date Dimension

    Generates a comprehensive date dimension table covering a configurable
    date range. Default is 2015-2030.
#}

with date_spine as (
    -- Generate date range using Snowflake's generator function
    select
        dateadd(day, seq4(), '2015-01-01'::date) as date_day
    from table(generator(rowcount => 5844)) -- 16 years worth of days (2015-2030)
    where date_day <= '2030-12-31'::date
),

date_parts as (
    select
        date_day,

        -- Date components
        dayofweek(date_day) as day_of_week,
        dayname(date_day) as day_of_week_name,
        substr(dayname(date_day), 1, 3) as day_of_week_name_short,
        day(date_day) as day_of_month,
        dayofyear(date_day) as day_of_year,

        -- Week
        weekofyear(date_day) as week_of_year,
        dateadd('day', -dayofweek(date_day) + 1, date_day) as week_start_date,
        dateadd('day', 7 - dayofweek(date_day), date_day) as week_end_date,

        -- Month
        month(date_day) as month_of_year,
        monthname(date_day) as month_name,
        substr(monthname(date_day), 1, 3) as month_name_short,
        date_trunc('month', date_day) as month_start_date,
        last_day(date_day) as month_end_date,

        -- Quarter
        quarter(date_day) as quarter_of_year,
        date_trunc('quarter', date_day) as quarter_start_date,
        last_day(date_trunc('quarter', date_day), 'quarter') as quarter_end_date,

        -- Year
        year(date_day) as year_number,
        date_trunc('year', date_day) as year_start_date,
        dateadd('day', -1, dateadd('year', 1, date_trunc('year', date_day))) as year_end_date

    from date_spine
)

select
    -- Primary Key
    date_day as date_key,

    -- Date Components
    date_day,
    day_of_week,
    day_of_week_name,
    day_of_week_name_short,
    day_of_month,
    day_of_year,

    -- Week
    week_of_year,
    week_start_date,
    week_end_date,

    -- Month
    month_of_year,
    month_name,
    month_name_short,
    month_start_date,
    month_end_date,

    -- Quarter
    quarter_of_year,
    quarter_start_date,
    quarter_end_date,

    -- Year
    year_number,
    year_start_date,
    year_end_date,

    -- Fiscal (assuming calendar year = fiscal year, adjust as needed)
    month_of_year as fiscal_month_of_year,
    quarter_of_year as fiscal_quarter_of_year,
    year_number as fiscal_year_number,

    -- Flags
    case when day_of_week in (0, 6) then true else false end as is_weekend,
    case when day_of_week not in (0, 6) then true else false end as is_weekday,
    case when date_day = current_date() then true else false end as is_today,

    -- Relative Periods
    datediff('day', date_day, current_date()) as days_ago,
    datediff('week', date_day, current_date()) as weeks_ago,
    datediff('month', date_day, current_date()) as months_ago,
    datediff('year', date_day, current_date()) as years_ago,

    -- Period Descriptions
    to_char(date_day, 'YYYY-MM') as year_month,
    to_char(date_day, 'YYYY') || '-Q' || quarter_of_year as year_quarter,
    to_char(date_day, 'Mon YYYY') as month_year_name

from date_parts
