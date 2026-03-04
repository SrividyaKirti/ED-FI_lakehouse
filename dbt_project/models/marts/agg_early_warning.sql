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

-- Detect declining trend: compare recent assessments to earlier ones
mastery_trend as (

    select
        student_id,
        avg(case when assessment_count <= 3 then max_score_to_date end) as early_avg,
        avg(case when assessment_count >= assessment_count - 2 then max_score_to_date end) as recent_avg

    from latest_mastery
    group by student_id

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
