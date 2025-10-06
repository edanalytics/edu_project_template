{{
  config(
    materialized="table",
    alias="tcap_courses",
    schema="vendor_pearson"
  )
}}

select distinct
    co.sourcedId,
    co.status,
    co.dateLastModified,
    co.schoolYearSourcedId,
    co.title,
    co.courseCode,
    co.grades,
    co.orgSourcedId,
    co.subjects,
    co.subjectCodes
from {{ ref('or1_1__courses') }} co
join {{ ref('tcap_course_year_ranges') }} tcap
    on co.courseCode = tcap.course_code
join {{ ref('or1_1__classes') }} c
	on co.sourcedId = c.courseSourcedId
join {{ ref('or1_1__orgs') }} o
	on c.schoolSourcedId = o.sourcedId
join {{ ref('or1_1__orgs') }} d
	on d.sourcedId = o.parentSourcedId