with edfi_attendance as (

    select
        student_id,
        school_id,
        event_date,
        attendance_status,
        _source_system

    from {{ ref('stg_edfi__attendance') }}

)

select * from edfi_attendance
