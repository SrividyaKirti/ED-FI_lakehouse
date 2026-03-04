-- Kiddom's "max value" mastery method: highest score achieved wins
with assessment_scores as (

    select
        a.student_id,
        a.standard_code,
        a.assessment_date,
        cast(a.score as double) as score,
        a._source_system

    from {{ ref('int_assessments') }} a
    where a.score is not null

),

mastery_calc as (

    select
        student_id,
        standard_code,
        assessment_date as date_key,
        score,
        _source_system,
        max(score) over (
            partition by student_id, standard_code
            order by assessment_date
            rows between unbounded preceding and current row
        ) as max_score_to_date,
        count(*) over (
            partition by student_id, standard_code
            order by assessment_date
            rows between unbounded preceding and current row
        ) as assessment_count

    from assessment_scores

),

with_mastery_level as (

    select
        *,
        case
            when max_score_to_date >= 90 then 'Exceeding'
            when max_score_to_date >= 70 then 'Meeting'
            when max_score_to_date >= 50 then 'Developing'
            else 'Needs Intervention'
        end as mastery_level

    from mastery_calc

)

select * from with_mastery_level
