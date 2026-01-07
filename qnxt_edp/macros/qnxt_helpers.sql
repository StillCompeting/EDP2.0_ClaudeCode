{#
    QNXT-Specific Helper Macros

    Common transformations and utilities for working with QNXT data.
#}

{% macro clean_qnxt_string(column_name) %}
    {# Trims whitespace and handles empty strings as NULL #}
    nullif(trim({{ column_name }}), '')
{% endmacro %}


{% macro parse_qnxt_date(column_name) %}
    {#
        Parses QNXT date columns which may be stored as VARCHAR or numeric.
        Handles common QNXT date formats: YYYYMMDD, YYYY-MM-DD
    #}
    case
        when {{ column_name }} is null then null
        when trim({{ column_name }}::varchar) = '' then null
        when trim({{ column_name }}::varchar) = '0' then null
        when length(trim({{ column_name }}::varchar)) = 8
            then try_to_date(trim({{ column_name }}::varchar), 'YYYYMMDD')
        else try_to_date(trim({{ column_name }}::varchar))
    end
{% endmacro %}


{% macro parse_qnxt_datetime(column_name) %}
    {# Parses QNXT datetime columns #}
    case
        when {{ column_name }} is null then null
        when trim({{ column_name }}::varchar) = '' then null
        else try_to_timestamp(trim({{ column_name }}::varchar))
    end
{% endmacro %}


{% macro qnxt_amount(column_name, default_value=0) %}
    {# Safely converts QNXT amount columns, defaulting NULL to specified value #}
    coalesce(
        case
            when {{ column_name }} is null then null
            when {{ column_name }}::varchar = '' then null
            else {{ column_name }}::number(18,2)
        end,
        {{ default_value }}
    )
{% endmacro %}


{% macro format_member_id(column_name) %}
    {# Standardizes QNXT member ID format - trims and uppercases #}
    upper(trim({{ column_name }}))
{% endmacro %}


{% macro format_provider_id(column_name) %}
    {# Standardizes QNXT provider ID format #}
    upper(trim({{ column_name }}))
{% endmacro %}


{% macro format_claim_id(column_name) %}
    {# Standardizes QNXT claim ID format #}
    upper(trim({{ column_name }}))
{% endmacro %}


{% macro derive_claim_status(status_column) %}
    {# Maps QNXT claim status codes to human-readable values #}
    case {{ status_column }}
        when 'A' then 'Approved'
        when 'D' then 'Denied'
        when 'P' then 'Pending'
        when 'V' then 'Void'
        when 'S' then 'Suspended'
        when 'I' then 'In Process'
        when 'R' then 'Rejected'
        else 'Unknown'
    end
{% endmacro %}


{% macro derive_gender(gender_column) %}
    {# Standardizes QNXT gender codes #}
    case upper(trim({{ gender_column }}))
        when 'M' then 'Male'
        when 'F' then 'Female'
        when 'U' then 'Unknown'
        else 'Unknown'
    end
{% endmacro %}


{% macro derive_member_status(status_column) %}
    {# Maps QNXT member status codes #}
    case upper(trim({{ status_column }}))
        when 'A' then 'Active'
        when 'T' then 'Terminated'
        when 'P' then 'Pending'
        when 'C' then 'Cancelled'
        else 'Unknown'
    end
{% endmacro %}


{% macro derive_line_of_business(lob_column) %}
    {# Maps QNXT line of business codes to standard names #}
    case upper(trim({{ lob_column }}))
        when 'COM' then 'Commercial'
        when 'MCD' then 'Medicaid'
        when 'MCR' then 'Medicare'
        when 'MKT' then 'Marketplace'
        when 'SNP' then 'Special Needs Plan'
        else {{ lob_column }}
    end
{% endmacro %}


{% macro calculate_age(birth_date_column, as_of_date=none) %}
    {# Calculates age from birth date, optionally as of a specific date #}
    {% set reference_date = as_of_date if as_of_date else 'current_date()' %}
    case
        when {{ birth_date_column }} is null then null
        else floor(datediff('day', {{ birth_date_column }}, {{ reference_date }}) / 365.25)
    end
{% endmacro %}


{% macro calculate_member_months(start_date, end_date) %}
    {# Calculates member months between two dates #}
    case
        when {{ start_date }} is null or {{ end_date }} is null then 0
        else greatest(datediff('month', {{ start_date }}, {{ end_date }}) + 1, 0)
    end
{% endmacro %}


{% macro is_valid_npi(npi_column) %}
    {# Validates NPI format (10 digits) #}
    length(trim({{ npi_column }})) = 10
    and regexp_like(trim({{ npi_column }}), '^[0-9]+$')
{% endmacro %}


{% macro standardize_phone(phone_column) %}
    {# Extracts digits only from phone number #}
    regexp_replace({{ phone_column }}, '[^0-9]', '')
{% endmacro %}


{% macro standardize_zip(zip_column) %}
    {# Standardizes ZIP code to 5-digit format #}
    left(regexp_replace({{ zip_column }}, '[^0-9]', ''), 5)
{% endmacro %}
