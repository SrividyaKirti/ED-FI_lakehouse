-- DQ Gate: Every enrollment school_id must exist in dim_school.
-- Returns orphaned enrollments with invalid school IDs.
-- Expected: FAILS on planted invalid SchoolIDs in generated data.

select e.student_id, e.school_id, e._source_system
from {{ ref('int_enrollments') }} e
left join {{ ref('dim_school') }} s on e.school_id = s.school_id
where s.school_id is null
