{% macro normalize_grade_level(raw_grade_col) %}
    case
        -- Ed-Fi descriptors
        when lower({{ raw_grade_col }}) in ('kindergarten', 'k', 'pk') then 0
        when lower({{ raw_grade_col }}) = 'first grade' then 1
        when lower({{ raw_grade_col }}) = 'second grade' then 2
        when lower({{ raw_grade_col }}) = 'third grade' then 3
        when lower({{ raw_grade_col }}) = 'fourth grade' then 4
        when lower({{ raw_grade_col }}) = 'fifth grade' then 5
        when lower({{ raw_grade_col }}) = 'sixth grade' then 6
        when lower({{ raw_grade_col }}) = 'seventh grade' then 7
        when lower({{ raw_grade_col }}) = 'eighth grade' then 8
        when lower({{ raw_grade_col }}) in ('ninth grade', 'freshman') then 9
        when lower({{ raw_grade_col }}) in ('tenth grade', 'sophomore') then 10
        when lower({{ raw_grade_col }}) in ('eleventh grade', 'junior') then 11
        when lower({{ raw_grade_col }}) in ('twelfth grade', 'senior') then 12
        -- OneRoster numeric formats ("00", "01", etc.)
        when {{ raw_grade_col }} ~ '^[0-9]+$' then cast({{ raw_grade_col }} as integer)
        else null
    end
{% endmacro %}
