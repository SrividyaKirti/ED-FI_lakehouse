with edfi_schools as (

    select
        school_id,
        school_name,
        school_type,
        district_id,
        district_name,
        _source_system

    from {{ ref('stg_edfi__schools') }}

),

oneroster_schools as (

    select
        org_id as school_id,
        org_name as school_name,
        -- OneRoster does not carry school_type directly
        null as school_type,
        parent_sourced_id as district_id,
        -- District name would require a self-join; seed_school_registry has the full data
        null as district_name,
        _source_system

    from {{ ref('stg_oneroster__orgs') }}
    where org_type = 'school'

),

unified as (

    select * from edfi_schools
    union all
    select * from oneroster_schools

)

select * from unified
