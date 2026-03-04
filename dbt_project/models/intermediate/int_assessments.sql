with edfi_assessments as (

    select
        student_id,
        assessment_id,
        question_number,
        standard_code,
        correct_answer,
        student_answer,
        score,
        assessment_date,
        misconception_indicator,
        _source_system

    from {{ ref('stg_edfi__assessments') }}

),

oneroster_assessments as (

    select
        r.student_sourced_id as student_id,
        r.line_item_sourced_id as assessment_id,
        r.question_number,
        r.standard_code,
        r.correct_answer,
        r.student_answer,
        cast(r.score as double) as score,
        r.score_date as assessment_date,
        r.misconception_indicator,
        r._source_system

    from {{ ref('stg_oneroster__results') }} r

),

unified as (

    select * from edfi_assessments
    union all
    select * from oneroster_assessments

)

select * from unified
