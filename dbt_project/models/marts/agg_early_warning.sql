-- Early warning system combining mastery trends and attendance
with latest_mastery as (

    -- Use only the latest mastery record per student per standard
    select
        m.student_id,
        m.standard_code,
        m.max_score_to_date,
        m.mastery_level,
        m.assessment_count

    from {{ ref('fact_student_mastery_daily') }} m
    inner join (
        select student_id, standard_code, max(date_key) as max_date
        from {{ ref('fact_student_mastery_daily') }}
        group by student_id, standard_code
    ) latest
        on m.student_id = latest.student_id
        and m.standard_code = latest.standard_code
        and m.date_key = latest.max_date

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
            row_number() over (partition by student_id order by date_key) as rn,
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
