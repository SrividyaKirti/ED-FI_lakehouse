with edfi_grades as (

    select
        student_id,
        section_id,
        grading_period,
        numeric_grade,
        _source_system

    from {{ ref('stg_edfi__grades') }}

)

select * from edfi_grades
