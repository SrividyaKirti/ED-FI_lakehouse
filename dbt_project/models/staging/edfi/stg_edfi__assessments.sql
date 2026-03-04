with source as (

    select * from {{ source('edfi_silver', 'assessment_results') }}

),

renamed as (

    select
        student_unique_id as student_id,
        assessment_id,
        question_number,
        standard_code,
        correct_answer,
        student_answer,
        score,
        assessment_date,
        misconception_indicator,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
