{{
  config(
    materialized="table",
    schema="stg_data_errors"
  )
}}

{% set error_code = 3201 %}

with stg_discipline_actions as (
    select * 
    from {{ ref('stg_ef3__discipline_actions_orig') }} da
    where 1=1
        {{ school_year_exists(error_code, 'da') }}
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
    join teds_dev.dev_smckee_stage.stg_ef3__discipline_incidents di
        on di.k_school = da.k_school__responsibility
        and di.incident_id = da.incidentIdentifier
)
/* Disipline Incident Date must be less than or equal to Discipline Action Date. */
select da.k_student, da.k_school__responsibility, da.school_year,
    da.discipline_action_id, da.discipline_date, da.responsibility_school_id,
    da.student_unique_id,
    {{ error_code }} as error_code,
    concat('Discipline Incident Date must be less than or equal to Discipline Action Date. Incident Identifier: ', 
        did.incidentIdentifier, ', Incident Date: ', did.incident_date, ', Discipline Date: ', 
        da.discipline_date, '.') as error,
    {{ error_severity_column(error_code, 'da') }}
from stg_discipline_actions da
inner join discipline_incident_dates did
    on did.k_school__responsibility = da.k_school__responsibility
    and did.school_year = da.school_year
    and did.k_student = da.k_student
    and did.discipline_action_id = da.discipline_action_id
    and did.discipline_date = da.discipline_date
    and did.incident_date > da.discipline_date