-- DQ Gate: Enrollment start dates must not be in the future.
-- Returns rows where enrollment_start_date is after today.
-- Expected: FAILS on planted future enrollment dates.

select student_id, enrollment_start_date, _source_system
from {{ ref('int_enrollments') }}
where cast(enrollment_start_date as date) > current_date
