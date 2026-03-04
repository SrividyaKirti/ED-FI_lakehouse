with source as (

    select * from {{ source('edfi_silver', 'standards') }}

),

renamed as (

    select
        standard_code,
        standard_description,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
