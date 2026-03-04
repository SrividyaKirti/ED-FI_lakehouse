with edfi_staff as (

    select
        staff_id,
        -- Hash Ed-Fi staff PII at the intermediate layer for consistency
        -- with OneRoster (which was hashed at Bronze→Silver by PySpark)
        sha256(lower(trim(first_name))) as first_name_hash,
        sha256(lower(trim(last_name))) as last_name_hash,
        sha256(lower(trim(email))) as email_hash,
        _source_system

    from {{ ref('stg_edfi__staff') }}

),

oneroster_staff as (

    select
        sourced_id as staff_id,
        given_name_hash as first_name_hash,
        family_name_hash as last_name_hash,
        email_hash,
        _source_system

    from {{ ref('stg_oneroster__users') }}
    where role = 'teacher'

),

unified as (
    select * from edfi_staff
    union all
    select * from oneroster_staff
)

select * from unified
