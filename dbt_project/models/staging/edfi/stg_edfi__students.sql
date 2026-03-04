with source as (

    select * from {{ source('edfi_silver', 'students') }}

),

renamed as (

    select
        student_unique_id as student_id,
        first_name_hash,
        last_name_hash,
        email_hash,
        birth_year,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
