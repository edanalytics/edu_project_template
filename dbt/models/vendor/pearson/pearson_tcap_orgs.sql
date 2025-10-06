{{
  config(
    materialized="table",
    alias="tcap_orgs",
    schema="vendor_pearson"
  )
}}

select distinct o.sourcedId, o.status, o.dateLastModified, o.name, o.type, o.identifier, o.parentSourcedId
from {{ ref('or1_1__orgs') }} o
join {{ ref('or1_1__orgs') }} d
	  on d.sourcedId = o.parentSourcedId
where o.type in ('district', 'school')
    and NOT (
        o.type = 'school'
        and SUBSTRING(o.identifier, LEN(o.identifier) - 3, 1) = '4'
    )
order by identifier