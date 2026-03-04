-- DQ Gate: Every student record must have a non-null student_id.
-- Returns counts of null student_ids grouped by source system.
-- Expected: FAILS on planted null student IDs.

select _source_system, count(*) as null_count
from {{ ref('int_students') }}
where student_id is null
group by _source_system
