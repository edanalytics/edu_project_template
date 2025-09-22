{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3300 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_attendance as (
    select k_student, k_school, k_session, 
        cast(school_year as int) as school_year, 
        school_id, student_unique_id, attendance_event_date, attendance_event_category
    from {{ ref('stg_ef3__student_school_attendance_events_orig') }} ssae
    where attendance_event_category = 'Student Standard Day'
        and exists (
        select 1
        from brule
        where cast(ssae.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
/* Student Standard Day events must be within enrollment period. */
select ssd.k_student, ssd.k_school, ssd.k_session, ssd.school_year, cast(ssd.school_id as int) as school_id, ssd.student_unique_id,
    ssd.attendance_event_date, ssd.attendance_event_category,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Student Standard Day for Student ', 
        ssd.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        'does not fall within Enrollment Period. Enrollment Start Date: ',
        ifnull(ssa.entry_date, '[null]'), ', Enrollment End Date: ', 
        ifnull(ssa.exit_withdraw_date, '[null]'), ', Student Standard Day Effective Date: ', 
        ssd.attendance_event_date, '.') as error,
    brule.tdoe_severity as severity
from stg_attendance ssd
join {{ ref('stg_ef3__students') }} s
    on s.k_student = ssd.k_student
left outer join {{ ref('stg_ef3__student_school_associations_orig') }} ssa
    on ssa.k_student = ssd.k_student
    and ssa.k_school = ssd.k_school
    and ssa.school_year = cast(ssd.school_year as int)
    /* No shows don't count. */
    --and ssa.entry_date < ifnull(ssa.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd'))
join brule
    on ssd.school_year between brule.error_school_year_start and brule.error_school_year_end
where (
        ssa.k_student is null
        or (ssa.k_student is not null 
            and not(ssd.attendance_event_date between ssa.entry_date 
                and ifnull(ssa.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd')))
            )
    )
    /* The date has to fit between some enrollment period even if it doesn't fit between EVERY enrollment 
        period for a student. */
    and not exists (
        select 1
        from {{ ref('stg_ef3__student_school_associations_orig') }} x
        where x.k_student = ssd.k_student
            and x.k_school = ssd.k_school
            and x.school_year = cast(ssd.school_year as int)
            /* No shows don't count. */
            --and x.entry_date < ifnull(x.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd'))
            and ssd.attendance_event_date between x.entry_date 
                and ifnull(x.exit_withdraw_date, to_date('9999-12-31','yyyy-MM-dd'))
    )