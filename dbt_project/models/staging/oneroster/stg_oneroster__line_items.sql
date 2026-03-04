with source as (

    select * from {{ source('oneroster_silver', 'line_items') }}

),

renamed as (

    select
        sourced_id,
        title,
        assign_date,
        due_date,
        class_sourced_id,
        result_value_min,
        result_value_max,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
