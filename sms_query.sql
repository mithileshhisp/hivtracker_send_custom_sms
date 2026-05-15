## pill_pickup_sms_query for 1 day due date
pill_pickup_sms_query = """

SELECT 
    org.name AS org_name,
    psi.duedate::date AS due_date,
    tei.uid AS tei_uid,

    MAX(CASE 
        WHEN teav.trackedentityattributeid = 2618 
        THEN teav.value 
    END) AS SMS_consent,
    
	MAX(CASE 
        WHEN teav.trackedentityattributeid = 9636 
        THEN teav.value 
    END) AS Mobile_consent,
	
    MAX(CASE 
        WHEN teav.trackedentityattributeid = 2617 
        THEN teav.value 
    END) AS Mobile_number

FROM programstageinstance psi

INNER JOIN organisationunit org 
    ON org.organisationunitid = psi.organisationunitid

INNER JOIN programinstance pi 
    ON pi.programinstanceid = psi.programinstanceid 

INNER JOIN trackedentityinstance tei
    ON tei.trackedentityinstanceid = pi.trackedentityinstanceid

INNER JOIN trackedentityattributevalue teav_sms
    ON teav_sms.trackedentityinstanceid = pi.trackedentityinstanceid
    AND teav_sms.trackedentityattributeid = 2618
    AND teav_sms.value = 'true'
	
INNER JOIN trackedentityattributevalue mobile_consent
	ON mobile_consent.trackedentityinstanceid = pi.trackedentityinstanceid
	AND mobile_consent.trackedentityattributeid = 9636
	AND mobile_consent.value = 'available'
		
LEFT JOIN trackedentityattributevalue teav 
    ON teav.trackedentityinstanceid = pi.trackedentityinstanceid 

WHERE 
    psi.programstageid = 2537 
    AND psi.status = 'SCHEDULE'
	AND psi.duedate::date = CURRENT_DATE + INTERVAL '1 days'
GROUP BY 
    org.name,
    psi.duedate,
    tei.uid
ORDER BY psi.duedate DESC;

"""

## pill_pickup_sms_query for 7 day due date
pill_pickup_sms_query_7days = """

SELECT 
    org.name AS org_name,
    psi.duedate::date AS due_date,
    tei.uid AS tei_uid,

    MAX(CASE 
        WHEN teav.trackedentityattributeid = 2618 
        THEN teav.value 
    END) AS SMS_consent,
    
	MAX(CASE 
        WHEN teav.trackedentityattributeid = 9636 
        THEN teav.value 
    END) AS Mobile_consent,
	
    MAX(CASE 
        WHEN teav.trackedentityattributeid = 2617 
        THEN teav.value 
    END) AS Mobile_number

FROM programstageinstance psi

INNER JOIN organisationunit org 
    ON org.organisationunitid = psi.organisationunitid

INNER JOIN programinstance pi 
    ON pi.programinstanceid = psi.programinstanceid 

INNER JOIN trackedentityinstance tei
    ON tei.trackedentityinstanceid = pi.trackedentityinstanceid

INNER JOIN trackedentityattributevalue teav_sms
    ON teav_sms.trackedentityinstanceid = pi.trackedentityinstanceid
    AND teav_sms.trackedentityattributeid = 2618
    AND teav_sms.value = 'true'
	
INNER JOIN trackedentityattributevalue mobile_consent
	ON mobile_consent.trackedentityinstanceid = pi.trackedentityinstanceid
	AND mobile_consent.trackedentityattributeid = 9636
	AND mobile_consent.value = 'available'
		
LEFT JOIN trackedentityattributevalue teav 
    ON teav.trackedentityinstanceid = pi.trackedentityinstanceid 

WHERE 
    psi.programstageid = 2537 
    AND psi.status = 'SCHEDULE'

    -- ✅ next 7 days
	AND psi.duedate::date = CURRENT_DATE + INTERVAL '7 days'

GROUP BY 
    org.name,
    psi.duedate,
    tei.uid
ORDER BY psi.duedate DESC;

"""

## awarenessmessages
awarenessmessages = """


SELECT 
    org.name AS org_name,
    tei.uid AS tei_uid,

    MAX(CASE 
        WHEN teav.trackedentityattributeid = 2618 
        THEN teav.value 
    END) AS SMS_consent,
    
    MAX(CASE 
        WHEN teav.trackedentityattributeid = 9636 
        THEN teav.value 
    END) AS Mobile_consent,
    
    MAX(CASE 
        WHEN teav.trackedentityattributeid = 2617 
        THEN teav.value 
    END) AS Mobile_number

FROM programinstance pi

INNER JOIN organisationunit org 
    ON org.organisationunitid = pi.organisationunitid

INNER JOIN trackedentityinstance tei
    ON tei.trackedentityinstanceid = pi.trackedentityinstanceid

INNER JOIN trackedentityattributevalue teav_sms
    ON teav_sms.trackedentityinstanceid = pi.trackedentityinstanceid
    AND teav_sms.trackedentityattributeid = 2618
    AND teav_sms.value = 'true'

INNER JOIN trackedentityattributevalue mobile_consent
    ON mobile_consent.trackedentityinstanceid = pi.trackedentityinstanceid
    AND mobile_consent.trackedentityattributeid = 9636
    AND mobile_consent.value = 'available'

LEFT JOIN trackedentityattributevalue teav 
    ON teav.trackedentityinstanceid = pi.trackedentityinstanceid 

WHERE org.organisationunitid IN (
    SELECT organisationunitid 
    FROM orgunitgroupmembers
    WHERE orgunitgroupid IN (
        SELECT orgunitgroupid 
        FROM orgunitgroup 
        WHERE uid = 'pW6owR4oRKb'
    )
)

GROUP BY 
    org.name,
    tei.uid

HAVING 
    MAX(CASE 
        WHEN teav.trackedentityattributeid = 2617 
        THEN teav.value 
    END) ~ '^[0-9]{10}$';

"""

## viral_load_awareness_messages
viral_load_awareness_messages = """

SELECT 
    org.name AS org_name,
    tei.uid AS tei_uid,

    MAX(CASE WHEN teav.trackedentityattributeid = 2618 THEN teav.value END) AS SMS_consent,
    MAX(CASE WHEN teav.trackedentityattributeid = 9636 THEN teav.value END) AS Mobile_consent,
    MAX(CASE WHEN teav.trackedentityattributeid = 2617 THEN teav.value END) AS Mobile_number

FROM programstageinstance psi

INNER JOIN organisationunit org 
    ON org.organisationunitid = psi.organisationunitid

INNER JOIN programinstance pi 
    ON pi.programinstanceid = psi.programinstanceid 

INNER JOIN trackedentityinstance tei
    ON tei.trackedentityinstanceid = pi.trackedentityinstanceid

LEFT JOIN trackedentityattributevalue teav 
    ON teav.trackedentityinstanceid = pi.trackedentityinstanceid 
    AND teav.trackedentityattributeid IN (2618, 9636, 2617)

WHERE 
    psi.programstageid IN (
        SELECT programstageid 
        FROM programstage 
        WHERE uid = 'YRSdePjzzfs'
    )

    AND psi.eventdatavalues::jsonb -> 'WpBa1L6xxPC' ->> 'value' = 'on_treatment'
    AND (psi.eventdatavalues::jsonb -> 'ji97ypBaP2i' ->> 'value')::numeric > 1000
	
GROUP BY 
    org.name,
    tei.uid

HAVING 
    MAX(CASE WHEN teav.trackedentityattributeid = 2618 THEN teav.value END) = 'true'
    AND MAX(CASE WHEN teav.trackedentityattributeid = 9636 THEN teav.value END) = 'available'
    AND MAX(CASE WHEN teav.trackedentityattributeid = 2617 THEN teav.value END) ~ '^[0-9]{10}$';
    
"""


## PregnancyAndDelivery
PregnancyAndDelivery = """

SELECT 
    org.name AS org_name,
    tei.uid AS tei_uid,

    MAX(CASE WHEN teav.trackedentityattributeid = 2618 THEN teav.value END) AS SMS_consent,
    MAX(CASE WHEN teav.trackedentityattributeid = 9636 THEN teav.value END) AS Mobile_consent,
    MAX(CASE WHEN teav.trackedentityattributeid = 2617 THEN teav.value END) AS Mobile_number

FROM programstageinstance psi

INNER JOIN organisationunit org 
    ON org.organisationunitid = psi.organisationunitid

INNER JOIN programinstance pi 
    ON pi.programinstanceid = psi.programinstanceid 

INNER JOIN trackedentityinstance tei
    ON tei.trackedentityinstanceid = pi.trackedentityinstanceid

LEFT JOIN trackedentityattributevalue teav 
    ON teav.trackedentityinstanceid = pi.trackedentityinstanceid 

WHERE 
    psi.programstageid in( select  programstageid from programstage where uid = 's7NCcAyCwp8')

GROUP BY 
    org.name,
    psi.duedate,
    tei.uid

HAVING 
    MAX(CASE WHEN teav.trackedentityattributeid = 2618 THEN teav.value END) = 'true'
    AND MAX(CASE WHEN teav.trackedentityattributeid = 9636 THEN teav.value END) = 'available'
    AND MAX(CASE WHEN teav.trackedentityattributeid = 2613 THEN teav.value END) = 'Female'
    AND MAX(CASE WHEN teav.trackedentityattributeid = 2617 THEN teav.value END) ~ '^[0-9]{10}$';
"""