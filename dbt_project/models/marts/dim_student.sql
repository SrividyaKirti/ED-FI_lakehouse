with students as (

    select
        s.student_id,
        s.first_name_hash,
        s.last_name_hash,
        s.email_hash,
        s.birth_year,
        e.school_id,
        e.grade_level,
        s._source_system

    from {{ ref('int_students') }} s
    left join {{ ref('int_enrollments') }} e
        on s.student_id = e.student_id

)

select * from students
