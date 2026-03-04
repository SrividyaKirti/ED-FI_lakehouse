select
    student_id,
    school_id,
    event_date,
    attendance_status as status,
    _source_system

from {{ ref('int_attendance') }}
