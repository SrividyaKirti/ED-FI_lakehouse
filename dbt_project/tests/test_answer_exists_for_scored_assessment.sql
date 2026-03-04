-- DQ Gate: Assessments with a non-null score must have both
-- correct_answer and student_answer populated.
-- Missing answers on scored items indicate incomplete assessment records.
-- Expected: PASSES — all scored assessments have answers.

select student_id, assessment_id, question_number, score, _source_system
from {{ ref('fact_assessment_responses') }}
where score is not null
  and (correct_answer is null or student_answer is null)
