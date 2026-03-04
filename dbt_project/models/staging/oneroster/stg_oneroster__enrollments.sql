with source as (

    select * from {{ source('oneroster_silver', 'enrollments') }}

),

renamed as (

    select
        sourced_id,
        class_sourced_id,
        school_sourced_id,
        user_sourced_id,
        role,
        begin_date,
        end_date,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
