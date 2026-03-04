with schools as (

    select
        s.school_id,
        s.school_name,
        coalesce(s.school_type, r.school_type) as school_type,
        coalesce(s.district_id, r.district_id) as district_id,
        coalesce(s.district_name, r.district_name) as district_name,
        r.grade_band_low,
        r.grade_band_high,
        s._source_system

    from {{ ref('int_schools') }} s
    left join {{ ref('seed_school_registry') }} r
        on s.school_id = r.school_id

)

select * from schools
