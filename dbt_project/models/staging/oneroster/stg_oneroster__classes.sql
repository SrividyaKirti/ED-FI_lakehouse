with source as (

    select * from {{ source('oneroster_silver', 'classes') }}

),

renamed as (

    select
        sourced_id,
        title,
        grades,
        course_sourced_id,
        class_code,
        class_type,
        school_sourced_id,
        term_sourced_ids,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
