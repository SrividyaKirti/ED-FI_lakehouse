with source as (

    select * from {{ source('edfi_silver', 'schools') }}

),

renamed as (

    select
        school_id,
        school_name,
        school_type,
        district_id,
        district_name,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
