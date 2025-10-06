{{
  config(
    materialized="table",
    alias="tcap_enrollments",
    schema="vendor_pearson"
  )
}}

select distinct
    e.sourcedId,
    e.status,
    e.dateLastModified,
    e.classSourcedId,
    e.schoolSourcedId,
    e.userSourcedId,
    e.role,
    case
        when e.role = 'teacher' then e.primary
        when e.role = 'student' then true
    end as primary,
    e.beginDate,
    e.endDate
from {{ ref('or1_1__enrollments') }} e
join {{ ref('or1_1__users') }} u
	 on e.userSourcedId = u.sourcedId
join {{ ref('or1_1__classes') }} c
	  on e.classSourcedId = c.sourcedId
join {{ ref('or1_1__courses') }} co
	  on co.sourcedId = c.courseSourcedId
join {{ ref('or1_1__academic_sessions') }} acs
	  on acs.sourcedId = c.termSourcedIds
join {{ ref('or1_1__orgs') }} o
	  on c.schoolSourcedId = o.sourcedId
join {{ ref('or1_1__orgs') }} d
	  on d.sourcedId = o.parentSourcedId
join {{ ref('tcap_course_year_ranges') }} tcap
    on co.courseCode = tcap.course_code
    and acs.title = tcap.academic_session
where coalesce(u.grades, 'XX') not in ('PK','KG','01')
	  and ((e.role = 'teacher' AND e.primary = 'true')
        OR e.role = 'student')