with invalid_school_ids as (

    select
        e._source_system as source_system,
        'enrollment' as entity_type,
        e.student_id as record_id,
        'valid_school_id' as rule_name,
        'SchoolID not found in district registry' as rule_description,
        'school_id' as field_name,
        cast(e.school_id as varchar) as field_value,
        'Must match a school in seed_school_registry' as expected_value,
        current_timestamp as quarantined_at

    from {{ ref('int_enrollments') }} e
    left join {{ ref('dim_school') }} s
        on e.school_id = s.school_id
    where s.school_id is null

),

future_enrollment_dates as (

    select
        _source_system as source_system,
        'enrollment' as entity_type,
        student_id as record_id,
        'enrollment_date_not_future' as rule_name,
        'Enrollment start date cannot be in the future' as rule_description,
        'enrollment_start_date' as field_name,
        cast(enrollment_start_date as varchar) as field_value,
        'Must be <= current_date' as expected_value,
        current_timestamp as quarantined_at

    from {{ ref('int_enrollments') }}
    where cast(enrollment_start_date as date) > current_date

),

null_student_ids as (

    select
        _source_system as source_system,
        'student' as entity_type,
        'UNKNOWN' as record_id,
        'student_id_not_null' as rule_name,
        'Student ID is required' as rule_description,
        'student_id' as field_name,
        'NULL' as field_value,
        'Must be non-null' as expected_value,
        current_timestamp as quarantined_at

    from {{ ref('int_students') }}
    where student_id is null

),

all_quarantined as (

    select * from invalid_school_ids
    union all
    select * from future_enrollment_dates
    union all
    select * from null_student_ids

)

select * from all_quarantined
