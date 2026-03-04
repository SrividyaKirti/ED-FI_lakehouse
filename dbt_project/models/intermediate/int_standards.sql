select
    standard_code,
    standard_description,
    domain,
    grade_level,
    prerequisite_standard_code

from {{ ref('seed_learning_standards') }}
