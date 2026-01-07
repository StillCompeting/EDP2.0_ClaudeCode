{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Medallion Architecture Schema Routing

        Routes models to appropriate schemas based on the custom_schema_name:
        - 'bronze' -> BRONZE schema (staging/raw models)
        - 'silver' -> SILVER schema (cleansed dimensions/facts)
        - 'gold'   -> GOLD schema (business marts)

        If no custom schema is specified, uses the target schema.
    #}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif custom_schema_name | lower == 'bronze' -%}
        {{ var('bronze_schema', 'BRONZE') }}

    {%- elif custom_schema_name | lower == 'silver' -%}
        {{ var('silver_schema', 'SILVER') }}

    {%- elif custom_schema_name | lower == 'gold' -%}
        {{ var('gold_schema', 'GOLD') }}

    {%- else -%}
        {# For any other custom schema, append to default #}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
