with edfi_enrollments as (

    select
        student_id,
        school_id,
        enrollment_start_date,
        {{ normalize_grade_level('grade_level_raw') }} as grade_level,
        _source_system

    from {{ ref('stg_edfi__enrollments') }}

),

oneroster_enrollments as (

    select
        e.user_sourced_id as student_id,
        e.school_sourced_id as school_id,
        e.begin_date as enrollment_start_date,
        {{ normalize_grade_level('c.grades') }} as grade_level,
        e._source_system

    from {{ ref('stg_oneroster__enrollments') }} e
    left join {{ ref('stg_oneroster__classes') }} c
        on e.class_sourced_id = c.sourced_id
    where e.role = 'student'

),

unified as (

    select * from edfi_enrollments
    union all
    select * from oneroster_enrollments

)

select * from unified
