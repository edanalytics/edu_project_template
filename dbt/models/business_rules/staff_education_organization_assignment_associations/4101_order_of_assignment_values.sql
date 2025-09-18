{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 4101 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_staff_edorg_assignment_assoc as (
    select * from {{ ref('stg_ef3__staff_education_organization_assignment_associations_orig') }} seoas
    where exists (
        select 1
        from brule
        where cast(seoas.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
)
/* Order of Assignment can be null or 1, 2, or 3 only. */
select seaa.k_staff, seaa.k_lea, seaa.k_school, seaa.school_year, seaa.ed_org_id, seaa.staff_unique_id,
    seaa.begin_date, seaa.staff_classification,
    brule.tdoe_error_code as error_code,
    concat('Order of Assignment can be [null], 1, 2, or 3. Value Received: ', 
        seaa.order_of_assignment, '.',
        ' Staff Email: ', coalesce(s.email_address, '[no value]')) as error,
    brule.tdoe_severity as severity
from stg_staff_edorg_assignment_assoc seaa
join {{ ref('dim_staff')}} s
    on s.k_staff = seaa.k_staff
join brule
    on seaa.school_year between brule.error_school_year_start and brule.error_school_year_end
where seaa.order_of_assignment is not null
    and seaa.order_of_assignment not in (1, 2, 3)