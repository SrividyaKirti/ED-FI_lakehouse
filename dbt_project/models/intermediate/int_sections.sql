with edfi_sections as (

    select
        section_id,
        school_id,
        course_name,
        curriculum_version,
        term_name,
        _source_system

    from {{ ref('stg_edfi__sections') }}

),

oneroster_sections as (

    select
        c.sourced_id as section_id,
        c.school_sourced_id as school_id,
        co.title as course_name,
        -- Extract curriculum version from class_code (e.g., "MATH-0-A" -> "A")
        split_part(c.class_code, '-', 3) as curriculum_version,
        null as term_name,
        c._source_system

    from {{ ref('stg_oneroster__classes') }} c
    left join {{ ref('stg_oneroster__courses') }} co
        on c.course_sourced_id = co.sourced_id

),

unified as (

    select * from edfi_sections
    union all
    select * from oneroster_sections

)

select * from unified
