{{
  config(
    materialized="table",
    schema="wh",
    post_hook=[ 
        "alter table {{ this }} alter column k_course_section set not null",
        "alter table {{ this }} alter column k_class_period set not null",
        "alter table {{ this }} alter column k_bell_schedule set not null",
        "alter table {{ this }} alter column calendar_date set not null",
        "alter table {{ this }} add primary key (k_course_section, k_class_period, k_bell_schedule, calendar_date)",
        "alter table {{ this }} add constraint fk_{{ this.name }}_course_section foreign key (k_course_section) references {{ ref('edu_wh', 'dim_course_section') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_class_period foreign key (k_class_period) references {{ ref('edu_wh', 'dim_class_period') }}",
        "alter table {{ this }} add constraint fk_{{ this.name }}_school foreign key (k_school) references {{ ref('edu_wh', 'dim_school') }}",
    ]
  )
}}

select scp.k_course_section, cp.k_class_period, cp.k_school, sec.k_course,
    cp.tenant_code, cp.school_year, 
    sec.educational_environment_type, sec.course_code, sec.is_cte,
    cp.class_period_name, cp.start_time, cp.end_time, cp.period_duration, 
    bs.k_bell_schedule, 
    bs.bell_schedule_name, bs.alternate_day_name, bs.calendar_date
from {{ ref('edu_wh', 'dim_class_period') }} cp
join (
        select bs.k_bell_schedule, bs.k_school, bs.school_year, bs.tenant_code, bs.api_year,
            bs.bell_schedule_name, bs.school_id, bs.alternate_day_name, bs.start_time, bs.end_time, bs.total_instructional_time,
            bscp.k_class_period, bscp.class_period_name, bsd.calendar_date
        from {{ ref('stg_ef3__bell_schedules') }} bs
        join {{ ref('edu_edfi_source', 'stg_ef3__bell_schedules__class_periods') }} bscp
            on bscp.tenant_code = bs.tenant_code
            and bscp.k_school = bs.k_school
            and bscp.k_bell_schedule = bs.k_bell_schedule
        join {{ ref('edu_edfi_source', 'stg_ef3__bell_schedules__dates') }} bsd
            on bsd.tenant_code = bs.tenant_code
            and bsd.k_school = bs.k_school
            and bsd.k_bell_schedule = bs.k_bell_schedule
    ) bs
    on bs.tenant_code = cp.tenant_code
    and bs.school_year = cp.school_year
    and bs.k_school = cp.k_school
    and bs.k_class_period = cp.k_class_period
join {{ ref('stg_ef3__sections__class_periods') }} scp
    on scp.k_class_period = cp.k_class_period
join {{ ref('dim_course_section') }} sec
    on sec.k_course_section = scp.k_course_section