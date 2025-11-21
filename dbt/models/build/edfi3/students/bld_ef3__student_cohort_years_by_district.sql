with student_cohort_years as (
    select * 
    from {{ ref('stg_ef3__stu_ed_org__cohort_years') }}
    where k_lea is not null
        and k_school is null
        and cohort_year_type = 'Ninth grade'
),
formatted as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_lea,
        ed_org_id,
        school_year as ninth_grade_cohort_year
    from student_cohort_years
)
select * from formatted