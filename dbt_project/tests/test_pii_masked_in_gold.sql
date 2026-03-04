-- DQ Gate: No raw PII columns should exist in the Gold layer dim_student.
-- Only hashed variants (first_name_hash, etc.) are permitted.
-- Returns column names that represent unmasked PII.
-- Expected: PASSES — Gold layer should only contain hashed PII.

select column_name
from information_schema.columns
where table_schema = 'gold'
  and table_name = 'dim_student'
  and column_name in (
      'first_name', 'last_name', 'email', 'birth_date',
      'student_name', 'given_name', 'family_name'
  )
