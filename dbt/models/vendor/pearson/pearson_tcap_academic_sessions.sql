{{
  config(
    materialized="table",
    alias="tcap_academic_sessions",
    schema="vendor_pearson"
  )
}}

select distinct acs.sourcedId, acs.status, acs.dateLastModified, tcap.test_window,
    'TestWindow' as type, acs.startDate, acs.endDate, acs.parentSourcedId, acs.schoolYear
from {{ ref('or1_1__academic_sessions') }} acs
join {{ ref('or1_1__classes') }} c
    on acs.sourcedId = c.termSourcedIds
join {{ ref('or1_1__orgs') }} o
    on c.schoolSourcedId = o.sourcedId
join {{ ref('or1_1__orgs') }} d
    on d.sourcedId = o.parentSourcedId
join {{ ref('or1_1__courses') }} co
    on co.sourcedId = c.courseSourcedId
join {{ ref('tcap_course_year_ranges') }} tcap
    on co.courseCode = tcap.course_code
    and acs.title = tcap.academic_session
    and acs.schoolYear between tcap.school_year_start and coalesce(tcap.school_year_end, 9999)
order by acs.parentSourcedId, tcap.test_window