{{
  config(
    materialized="table",
    schema="cds"
  )
}}

select s.k_staff, 
    s.record_guid as oid,
    c.credential_id as TeacherLicenseNumber,
    c.effective_date as TeacherLicenseEffectiveDate,
    c.expiration_date as TeacherLicenseExpirationDate,
    c.issuance_date as TeacherLicenseIssuanceDate
from {{ ref('stg_ef3__staffs') }} s
left outer join {{ ref('stg_ef3__staffs__credentials') }} sc
    on sc.k_staff = s.k_staff
left outer join {{ ref('stg_ef3__credentials') }} c
    on c.k_credential = sc.k_credential
    and c.credential_field = 'Generalist'
    and c.credential_type = 'Other'
    and c.teaching_credential = 'Other'
    and c.state_of_issue_state_abbreviation = 'TN'