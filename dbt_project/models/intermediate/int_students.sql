with edfi_students as (

    select
        student_id,
        first_name_hash,
        last_name_hash,
        email_hash,
        birth_year,
        _source_system

    from {{ ref('stg_edfi__students') }}

),

oneroster_students as (

    select
        sourced_id as student_id,
        given_name_hash as first_name_hash,
        family_name_hash as last_name_hash,
        email_hash,
        birth_year,
        _source_system

    from {{ ref('stg_oneroster__users') }}
    where role = 'student'

),

unified as (

    select * from edfi_students
    union all
    select * from oneroster_students

)

select * from unified
