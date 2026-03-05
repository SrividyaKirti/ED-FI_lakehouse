-- Early warning system combining mastery trends and attendance
with ranked_mastery as (

    select
        student_id,
        standard_code,
        max_score_to_date,
        mastery_level,
        assessment_count,
        row_number() over (
            partition by student_id, standard_code
            order by assessment_count desc, date_key desc nulls last
        ) as rn

    from {{ ref('fact_student_mastery_daily') }}

),

latest_mastery as (

    select
        student_id,
        standard_code,
        max_score_to_date,
        mastery_level,
        assessment_count

    from ranked_mastery
    where rn = 1

),

mastery_summary as (

    select
        student_id,
        avg(max_score_to_date) as avg_mastery_score,
        sum(case when max_score_to_date < 50 then 1 else 0 end) as count_below_developing,
        count(distinct standard_code) as standards_assessed

    from latest_mastery
    group by student_id

),

-- Detect declining trend: compare first-half vs second-half of assessments per student
mastery_trend as (

    select
        f.student_id,
        avg(case when f.rn <= f.total / 2 then f.max_score_to_date end) as early_avg,
        avg(case when f.rn > f.total / 2 then f.max_score_to_date end) as recent_avg
    from (
        select
            student_id,
            max_score_to_date,
            row_number() over (partition by student_id order by date_key nulls last) as rn,
            count(*) over (partition by student_id) as total
        from {{ ref('fact_student_mastery_daily') }}
    ) f
    where f.total >= 2
    group by f.student_id

),

attendance_rates as (

    select
        student_id,
        count(case when status = 'Present' then 1 end)::float / nullif(count(*), 0) as attendance_rate

    from {{ ref('fact_attendance_daily') }}
    group by student_id

),

combined as (

    select
        m.student_id,
        m.avg_mastery_score,
        coalesce(a.attendance_rate, 1.0) as attendance_rate,
        m.count_below_developing,
        m.standards_assessed,
        case when t.recent_avg < t.early_avg then true else false end as declining_trend,
        case
            when m.count_below_developing >= 3 and coalesce(a.attendance_rate, 1.0) < 0.90 then 'High'
            when m.count_below_developing >= 2 or coalesce(a.attendance_rate, 1.0) < 0.90 then 'Medium'
            else 'Low'
        end as risk_level

    from mastery_summary m
    left join mastery_trend t
        on m.student_id = t.student_id
    left join attendance_rates a
        on m.student_id = a.student_id

)

select * from combined
