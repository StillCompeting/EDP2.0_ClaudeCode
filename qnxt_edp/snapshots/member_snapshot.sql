{% snapshot member_snapshot %}
{#
    Member Snapshot (SCD Type 2)

    Tracks historical changes to member demographic information.
    Uses timestamp strategy based on the updated_at column.
#}

{{
    config(
        target_schema='silver',
        unique_key='member_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    member_id,
    first_name,
    middle_name,
    last_name,
    date_of_birth,
    gender_code,
    gender,
    ssn,
    medicaid_id,
    medicare_id,
    email,
    preferred_language,
    status_code,
    member_status,
    updated_at

from {{ ref('stg_qnxt__member') }}

{% endsnapshot %}
