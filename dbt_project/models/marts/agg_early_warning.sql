with mastery_summary as (

    select
        student_id,
        avg(max_score_to_date) as avg_mastery_score,
        sum(case when max_score_to_date < 50 then 1 else 0 end) as count_below_developing,
        count(distinct standard_code) as standards_assessed

    from {{ ref('fact_student_mastery_daily') }}
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
        case
            when m.count_below_developing >= 3 and coalesce(a.attendance_rate, 1.0) < 0.90 then 'High'
            when m.count_below_developing >= 2 or coalesce(a.attendance_rate, 1.0) < 0.90 then 'Medium'
            else 'Low'
        end as risk_level

    from mastery_summary m
    left join attendance_rates a
        on m.student_id = a.student_id

)

select * from combined
