with source as (

    select * from {{ source('edfi_silver', 'staff') }}

),

renamed as (

    select
        staff_unique_id as staff_id,
        first_name,
        last_name,
        email,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
