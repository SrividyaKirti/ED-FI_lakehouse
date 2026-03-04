-- DQ Gate: Each student_id should appear at most once per source system.
-- Duplicate records from the same source indicate an ETL dedup failure.
-- Expected: PASSES — no duplicate students in int_students.

select student_id, _source_system, count(*) as occurrence_count
from {{ ref('int_students') }}
where student_id is not null
group by student_id, _source_system
having count(*) > 1
