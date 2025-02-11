{{
  config(
    materialized="table",
    schema="stage"
  )
}}

with stg_staff as (
    select * from {{ ref('edu_edfi_source', 'stg_ef3__staffs') }}
),
flattened as (
    select s.tenant_code, s.api_year, s.k_staff, 
    c.value:credentialReference.credentialIdentifier::string as credential_identifier,
    {{ edu_edfi_source.extract_descriptor('c.value:credentialReference.stateOfIssueStateAbbreviationDescriptor::string') }} as state_of_issue
    from stg_staff s,
    lateral variant_explode(s.v_credentials) c
),
extended as (
    select tenant_code, api_year, k_staff,
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(credential_identifier)',
            'lower(state_of_issue)']
        ) }} as k_credential,
        credential_identifier, state_of_issue
    from flattened
)
select *
from extended