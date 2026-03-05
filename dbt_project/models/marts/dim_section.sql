select
    section_id,
    school_id,
    course_name,
    curriculum_version,
    term_name,
    subject,
    grade_level,
    _source_system

from {{ ref('int_sections') }}
