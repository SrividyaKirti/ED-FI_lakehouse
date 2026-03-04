with latest_mastery as (

    select
        m.student_id,
        m.standard_code,
        m.max_score_to_date,
        m.mastery_level,
        m._source_system

    from {{ ref('fact_student_mastery_daily') }} m
    inner join (
        select
            student_id,
            standard_code,
            max(date_key) as max_date
        from {{ ref('fact_student_mastery_daily') }}
        group by student_id, standard_code
    ) latest
        on m.student_id = latest.student_id
        and m.standard_code = latest.standard_code
        and m.date_key = latest.max_date

),

by_district as (

    select
        lm.standard_code,
        sch.district_id,
        sch.district_name,
        count(distinct lm.student_id) as student_count,
        avg(lm.max_score_to_date) as avg_score,
        avg(case when lm.mastery_level in ('Meeting', 'Exceeding') then 1.0 else 0.0 end) as mastery_pct

    from latest_mastery lm
    inner join {{ ref('dim_student') }} ds
        on lm.student_id = ds.student_id
    inner join {{ ref('dim_school') }} sch
        on ds.school_id = sch.school_id
    group by lm.standard_code, sch.district_id, sch.district_name

)

select * from by_district
