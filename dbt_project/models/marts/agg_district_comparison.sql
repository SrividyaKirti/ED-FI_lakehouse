with ranked_mastery as (

    select
        student_id,
        standard_code,
        max_score_to_date,
        mastery_level,
        _source_system,
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
        _source_system

    from ranked_mastery
    where rn = 1

),

by_district as (

    select
        lm.standard_code,
        std.subject,
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
    left join {{ ref('dim_standard') }} std
        on lm.standard_code = std.standard_code
    group by lm.standard_code, std.subject, sch.district_id, sch.district_name

)

select * from by_district
