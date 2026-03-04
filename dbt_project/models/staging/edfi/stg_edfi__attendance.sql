with source as (

    select * from {{ source('edfi_silver', 'attendance') }}

),

renamed as (

    select
        student_unique_id as student_id,
        school_id,
        event_date,
        attendance_status,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
