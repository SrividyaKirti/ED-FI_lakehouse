-- DQ Gate: Students should not be enrolled in schools outside their grade band.
-- Elementary students (K-5) should not appear in high-school-only buildings, etc.
-- Returns rows where a student's grade_level falls below the school's grade_band_low.

select
    e.student_id,
    e.grade_level,
    s.school_name,
    s.grade_band_low,
    s.grade_band_high,
    e._source_system
from {{ ref('int_enrollments') }} e
inner join {{ ref('dim_school') }} s on e.school_id = s.school_id
where e.grade_level is not null
  and s.grade_band_low is not null
  and e.grade_level < s.grade_band_low
