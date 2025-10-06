{{
  config(
    materialized="table",
    alias="tcap_demographics",
    schema="vendor_pearson"
  )
}}

select distinct
    d.sourcedId,
    d.status,
    d.dateLastModified,
    d.birthDate,
    d.sex,
    d.americanIndianOrAlaskaNative,
    d.asian,
    d.blackOrAfricanAmerican,
    d.nativeHawaiianOrOtherPacificIslander,
    d.white,
    d.demographicRaceTwoOrMoreRaces,
    d.hispanicOrLatinoEthnicity,
    if(s.is_economic_disadvantaged = 'true',1,2) AS CodeAB,
    if(sc.student_characteristic = 'I', 1, 0) AS isMigrant,
    if(sc.student_characteristic = 'T', 1, 0) AS Title1,
    CASE
        WHEN s.lep_code IN ('L', 'W') THEN 1
        ELSE 0
    END as isEL
FROM {{ ref('or1_1__demographics') }} d
JOIN {{ ref('or1_1__users') }} u
    ON d.sourcedId = u.sourcedId
JOIN {{ ref('or1_1__enrollments') }} e
    ON u.sourcedId = e.userSourcedId
JOIN {{ ref('or1_1__classes') }} c
    ON e.classSourcedId = c.sourcedId
JOIN {{ ref('or1_1__courses') }} co
    ON co.sourcedId = c.courseSourcedId
join {{ ref('or1_1__academic_sessions') }} acs
	on acs.sourcedId = c.termSourcedIds
join {{ ref('tcap_course_year_ranges') }} tcap
    on co.courseCode = tcap.course_code
    and acs.title = tcap.academic_session
    and acs.schoolYear between tcap.school_year_start and coalesce(tcap.school_year_end, 9999)
left JOIN {{ ref('dim_student') }} s
    ON u.identifier = s.student_unique_id
left JOIN {{ ref('fct_student_characteristics') }} sc
    ON s.k_student = sc.k_student AND sc.student_characteristic IN ('T','I')
WHERE u.role = 'student' 
    AND coalesce(u.grades, 'XX') NOT IN ('PK','KG','01')