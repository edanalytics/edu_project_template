{{
  config(
    materialized="table",
    alias="tcap_users",
    schema="vendor_pearson"
  )
}}

select distinct
    u.sourcedId,
    u.status,
    u.dateLastModified,
    u.enabledUser,
    u.orgSourcedIds,
    u.role,
    case
        when u.role = 'student' then ''
        else u.username
    end as username,
    case when u.role = 'student' then regexp_extract(u.userIDs, 'Legacy State Student Id:([^}]*)', 1)
        when u.role = 'teacher' then regexp_extract(u.userIDs, 'TLN:([^}]*)', 1)
        else ''
    end as userIDs,
    u.givenName,
    u.familyName,
    u.middleName,
    u.identifier,
    case when u.role = 'student' then ''
        else u.email
	  end as email,
    u.agentSourceIds,
    u.grades
from {{ ref('or1_1__users') }} u
where coalesce(u.grades, 'XX') not in ('PK','KG','01') 
order by role desc