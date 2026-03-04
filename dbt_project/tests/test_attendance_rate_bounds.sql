-- DQ Gate: Per-student attendance rates must be between 0 and 1 (0%-100%).
-- A rate outside these bounds indicates a computation or data error.
-- Expected: PASSES — all attendance rates should be valid.

with rates as (
    select
        student_id,
        count(case when status = 'Present' then 1 end)::float / nullif(count(*), 0) as rate
    from {{ ref('fact_attendance_daily') }}
    group by student_id
)
select * from rates
where rate < 0 or rate > 1
