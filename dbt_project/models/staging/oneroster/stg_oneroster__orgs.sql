with source as (

    select * from {{ source('oneroster_silver', 'orgs') }}

),

renamed as (

    select
        sourced_id as org_id,
        name as org_name,
        type as org_type,
        identifier,
        parent_sourced_id,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
