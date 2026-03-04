select
    misconception_id,
    standard_code,
    pattern_label,
    description,
    suggested_reteach,
    wrong_answer_pattern

from {{ ref('seed_misconception_patterns') }}
