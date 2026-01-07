{% snapshot eligibility_snapshot %}
{#
    Member Eligibility Snapshot (SCD Type 2)

    Tracks historical changes to member eligibility segments.
    Critical for point-in-time eligibility verification and
    historical coverage analysis.
#}

{{
    config(
        target_schema='silver',
        unique_key='eligibility_id',
        strategy='timestamp',
        updated_at='_loaded_at',
        invalidate_hard_deletes=True
    )
}}

select
    member_id,
    eligibility_id,
    effective_date,
    termination_date,
    lob_id,
    line_of_business,
    plan_id,
    plan_name,
    product_id,
    group_id,
    subgroup_id,
    coverage_type,
    relationship_code,
    subscriber_id,
    pcp_provider_id,
    eligibility_status_code,
    _loaded_at

from {{ ref('stg_qnxt__mbrelig') }}

{% endsnapshot %}
