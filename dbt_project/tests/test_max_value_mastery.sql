-- DQ Gate: max_score_to_date must be monotonically non-decreasing
-- per student per standard over time (the Kiddom "max value" mastery method).
-- A decrease in max_score_to_date indicates a calculation bug.
-- Expected: PASSES — the window function guarantees this property.

with ordered as (
    select
        student_id,
        standard_code,
        date_key,
        max_score_to_date,
        lag(max_score_to_date) over (
            partition by student_id, standard_code
            order by date_key
        ) as prev_max
    from {{ ref('fact_student_mastery_daily') }}
)
select * from ordered
where prev_max is not null and max_score_to_date < prev_max
