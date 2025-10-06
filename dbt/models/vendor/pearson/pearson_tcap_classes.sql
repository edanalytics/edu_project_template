{{
  config(
    materialized="table",
    alias="tcap_classes",
    schema="vendor_pearson"
  )
}}

select distinct
    c.sourcedId,
    c.status,
    c.dateLastModified,
    c.title,
    c.grades,
    c.courseSourcedId, c.classCode,
    c.classType,
    c.location,
    c.schoolSourcedId,
    c.termSourcedIds,
    c.subject,
    c.subjectCodes,
    c.periods
from {{ ref('or1_1__classes') }} c
join {{ ref('or1_1__courses') }} co
	on co.sourcedId = c.courseSourcedId
join {{ ref('or1_1__academic_sessions') }} acs
	on acs.sourcedId = c.termSourcedIds
join {{ ref('tcap_course_year_ranges') }} tcap
    on co.courseCode = tcap.course_code
    and acs.title = tcap.academic_session
    and acs.schoolYear between tcap.school_year_start and coalesce(tcap.school_year_end, 9999)
join {{ ref('or1_1__orgs') }} o
	on c.schoolSourcedId = o.sourcedId
join {{ ref('or1_1__orgs') }} d
	on d.sourcedId = o.parentSourcedId