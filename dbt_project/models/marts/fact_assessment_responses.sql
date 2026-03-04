with responses as (

    select
        a.student_id,
        a.assessment_id,
        a.question_number,
        a.standard_code,
        a.correct_answer,
        a.student_answer,
        a.score,
        a.assessment_date,
        case
            when a.student_answer is null or a.correct_answer is null then null
            when a.student_answer = a.correct_answer then true
            else false
        end as is_correct,
        a.misconception_indicator,
        mp.pattern_label as misconception_tag,
        mp.description as misconception_description,
        mp.suggested_reteach,
        a._source_system

    from {{ ref('int_assessments') }} a
    left join {{ ref('dim_misconception_pattern') }} mp
        on a.standard_code = mp.standard_code
        and a.student_answer != a.correct_answer
        and a.misconception_indicator = mp.pattern_label

)

select * from responses
