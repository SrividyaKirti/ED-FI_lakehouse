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
        o.org_id as school_id,
        o.org_name as school_name,
        -- Use seed_school_registry for school_type
        r.school_type,
        o.parent_sourced_id as district_id,
        -- Use seed_school_registry for district_name
        r.district_name,
        o._source_system

    from {{ ref('stg_oneroster__orgs') }} o
    left join {{ ref('seed_school_registry') }} r
        on o.identifier = r.school_id
    where o.org_type = 'school'

),

unified as (

    select * from edfi_schools
    union all
    select * from oneroster_schools

)

select * from unified
