with source as (

    select * from {{ source('oneroster_silver', 'results') }}

),

renamed as (

    select
        sourced_id,
        line_item_sourced_id,
        student_sourced_id,
        score,
        score_date,
        question_number,
        standard_code,
        correct_answer,
        student_answer,
        misconception_indicator,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
