with source as (

    select * from {{ source('edfi_silver', 'grades') }}

),

renamed as (

    select
        student_unique_id as student_id,
        section_identifier as section_id,
        grading_period,
        numeric_grade,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
