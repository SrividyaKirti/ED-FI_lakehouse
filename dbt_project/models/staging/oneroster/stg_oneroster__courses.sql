with source as (

    select * from {{ source('oneroster_silver', 'courses') }}

),

renamed as (

    select
        sourced_id,
        title,
        course_code,
        grades,
        org_sourced_id,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
