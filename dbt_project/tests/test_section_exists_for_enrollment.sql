-- DQ Gate: Every school with enrollments should have at least one section.
-- Schools that appear in enrollments but have zero sections are suspect.
-- Returns school_ids from enrollments with no matching sections.

select distinct e.school_id, e._source_system
from {{ ref('int_enrollments') }} e
left join {{ ref('int_sections') }} s on e.school_id = s.school_id
where s.school_id is null and e.school_id is not null
