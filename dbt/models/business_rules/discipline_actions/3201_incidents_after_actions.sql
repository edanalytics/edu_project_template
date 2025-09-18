{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3201 %}

with brule as (
    select tdoe_error_code, 
        cast(error_school_year_start as int) as error_school_year_start, 
        cast(ifnull(error_school_year_end, 9999) as int) as error_school_year_end,
        tdoe_severity
    from {{ ref('business_rules_year_ranges') }} br
    where br.tdoe_error_code = {{ error_code }}
),
stg_discipline_actions as (
    select * 
    from {{ ref('stg_ef3__discipline_actions_orig') }} da
    where exists (
        select 1
        from brule
        where cast(da.school_year as int) between brule.error_school_year_start and brule.error_school_year_end
    )
),
discipline_incidents_exploded as (
    select distinct k_student, k_school__responsibility, school_year, discipline_action_id, discipline_date, 
        ({{ edu_edfi_source.extract_descriptor('value:studentDisciplineIncidentAssociationReference::string') }}):incidentIdentifier::string as incidentIdentifier
    from stg_discipline_actions, 
        lateral variant_explode(v_student_discipline_incident_associations)
),
discipline_incident_dates as (
    select da.k_student, da.k_school__responsibility, da.school_year,
        da.discipline_action_id, da.discipline_date, da.incidentIdentifier,
        di.incident_date
    from discipline_incidents_exploded da
    join {{ ref('edu_edfi_source', 'stg_ef3__discipline_incidents') }} di
        on di.k_school = da.k_school__responsibility
        and di.incident_id = da.incidentIdentifier
)
/* Disipline Incident Date must be less than or equal to Discipline Action Date. */
select da.k_student, da.k_school__responsibility, da.school_year,
    da.discipline_action_id, da.discipline_date, da.responsibility_school_id,
    da.student_unique_id,
    s.state_student_id as legacy_state_student_id,
    brule.tdoe_error_code as error_code,
    concat('Discipline Incident Date for Student ', 
        da.student_unique_id, ' (', coalesce(s.state_student_id, '[no value]'), ') ',
        ' must be less than or equal to Discipline Action Date. Incident Identifier: ', 
        did.incidentIdentifier, ', Incident Date: ', did.incident_date, ', Discipline Date: ', 
        da.discipline_date, '.') as error,
    brule.tdoe_severity as severity
from stg_discipline_actions da
join {{ ref('stg_ef3__students') }} s
    on s.k_student = da.k_student
inner join discipline_incident_dates did
    on did.k_school__responsibility = da.k_school__responsibility
    and did.school_year = da.school_year
    and did.k_student = da.k_student
    and did.discipline_action_id = da.discipline_action_id
    and did.discipline_date = da.discipline_date
    and did.incident_date > da.discipline_date
join brule
    on da.school_year between brule.error_school_year_start and brule.error_school_year_end