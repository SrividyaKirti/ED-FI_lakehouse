with source as (

    select * from {{ source('oneroster_silver', 'users') }}

),

renamed as (

    select
        sourced_id,
        role,
        grades,
        given_name_hash,
        family_name_hash,
        email_hash,
        birth_year,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
