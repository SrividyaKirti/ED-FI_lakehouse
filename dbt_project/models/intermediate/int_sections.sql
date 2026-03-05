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

),

with_subject as (

    select
        *,
        case
            when course_name like '%Math%' then 'Math'
            when course_name like '%ELA%' then 'ELA'
            when course_name like '%Science%' then 'Science'
            else 'Unknown'
        end as subject,
        case
            when course_name like 'Kindergarten%' or course_name = 'Science K' then 0
            else try_cast(regexp_extract(course_name, '(\d+)$', 1) as int)
        end as grade_level

    from unified

)

select * from with_subject
