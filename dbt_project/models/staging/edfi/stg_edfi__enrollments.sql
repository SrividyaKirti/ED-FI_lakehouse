with source as (

    select * from {{ source('edfi_silver', 'enrollments') }}

),

renamed as (

    select
        student_unique_id as student_id,
        school_id,
        entry_date as enrollment_start_date,
        grade_level_descriptor as grade_level_raw,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
