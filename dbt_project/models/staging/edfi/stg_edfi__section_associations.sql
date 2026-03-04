with source as (

    select * from {{ source('edfi_silver', 'section_associations') }}

),

renamed as (

    select
        student_unique_id as student_id,
        section_identifier as section_id,
        begin_date,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
