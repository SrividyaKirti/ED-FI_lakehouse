with source as (

    select * from {{ source('oneroster_silver', 'demographics') }}

),

renamed as (

    select
        sourced_id,
        birth_date,
        sex,
        american_indian_or_alaska_native,
        asian,
        black_or_african_american,
        native_hawaiian_or_other_pacific_islander,
        white,
        demographic_race_two_or_more_races,
        hispanic_or_latino_ethnicity,
        _source_system,
        _loaded_at

    from source

)

select * from renamed
