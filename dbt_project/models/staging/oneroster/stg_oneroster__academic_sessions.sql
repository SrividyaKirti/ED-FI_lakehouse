with source as (

    select * from {{ source('oneroster_silver', 'academic_sessions') }}

),

renamed as (

    select
        sourced_id,
        title,
        type,
        start_date,
        end_date,
        parent_sourced_id,
        school_year,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
