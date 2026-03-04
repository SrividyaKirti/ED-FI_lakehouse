with source as (

    select * from {{ source('edfi_silver', 'sections') }}

),

renamed as (

    select
        section_identifier as section_id,
        school_id,
        course_name,
        curriculum_version,
        term_name,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
