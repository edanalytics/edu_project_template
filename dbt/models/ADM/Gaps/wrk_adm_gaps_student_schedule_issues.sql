{{
  config(
    materialized="table",
    schema="stg_adm_gaps"
  )
}}

select school_year, k_student, k_school, is_primary_school,
    'student schedule' as reason_type,
    count(*) as reason_count,
    concat('Student is scheduled for less minutes than their SSD indicates for the following Report Periods:\n',
        concat_ws('\n', collect_list(concat('\t', minutes_off)))) as errors
from (
    select school_year, k_student, k_school, is_primary_school, report_period,
        sum(sum_period_duration) as sum_periods,
        sum(ssd_duration) as sum_ssd,
        concat('Report Period ', report_period, ': ', 
            cast(sum(sum_period_duration) as int), ' section minutes vs. ',
            cast(sum(ssd_duration) as int), ' SSD minutes') as minutes_off
    from (
        select school_year, k_student, k_school, is_primary_school,
            report_period, calendar_date, ssd_duration, 
            sum(period_duration) as sum_period_duration
        from {{ ref('student_day_sections') }}
        where k_student = '95951531f447c169ac5a99bec36e9225'
        group by school_year, k_student, k_school, is_primary_school,
            report_period, calendar_date, ssd_duration
    )
    group by school_year, k_student, k_school, is_primary_school, report_period
    order by report_period
)
where sum_periods != sum_ssd
group by school_year, k_student, k_school, is_primary_school