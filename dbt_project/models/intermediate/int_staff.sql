with edfi_staff as (

    select
        staff_id,
        first_name,
        last_name,
        email,
        _source_system

    from {{ ref('stg_edfi__staff') }}

),

oneroster_staff as (

    select
        sourced_id as staff_id,
        given_name_hash as first_name,
        family_name_hash as last_name,
        email_hash as email,
        _source_system

    from {{ ref('stg_oneroster__users') }}
    where role = 'teacher'

)

select * from edfi_staff
union all
select * from oneroster_staff
