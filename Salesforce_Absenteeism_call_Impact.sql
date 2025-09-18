WITH call_data as
(
    select distinct
        "Student_Lead_Id" as student_id,
        "CLM_Query_Type" as call_type,
        get_timestamp_ist("ClosedDate"::text) as call_closed_date,
        TRIM(replace(split_part("Description",':',2),'seconds','')) as call_duration_seconds,
        "Lead_Grade" as student_grade
    from bhanzu_analytics.dynamo_prod_alltickets
    where 1=1
      and "CLM_Query_Type" = 'absenteeism_call'
      and "Priority" = 'medium'
      and "Status" = 'closed'
      and "Description" <> '..'
      and "GeoGraphy" = 'IND'
      and get_timestamp_ist("ClosedDate"::text) BETWEEN  '2025-08-01' AND '2025-08-31'
)

,sd as
(
SELECT 
    lead_id,
    batch_id,
    session_id,
    session_status,
    start_time_ist,
    is_present
FROM
    quicksight_mat_views.students_eligible_batch_sessions
WHERE
        session_status = 'completed' -- 'cancelled'
    and session_type = 'Batch Session'
    and start_time_ist BETWEEN '2025-07-01' and current_date
)


,PRE as
    (Select
            cd.*,
            sd.*,
            ROW_NUMBER()OVER(PARTITION BY student_id, call_closed_date order by start_time_ist desc) as pre_rn
    from
        call_data cd
    left join
        sd on cd.student_id = sd.lead_id AND sd.start_time_ist < cd.call_closed_date
    )

,POST as
    (Select
            cd.*,
            sd.*,
            ROW_NUMBER()OVER(PARTITION BY student_id, call_closed_date order by start_time_ist asc) as post_rn,
            ROW_NUMBER()OVER(PARTITION BY student_id, call_closed_date order by start_time_ist desc) as lat_post_rn
    from
        call_data cd
    left join
        sd on cd.student_id = sd.lead_id AND sd.start_time_ist > cd.call_closed_date
    )

,PRE_FINAL as
(
    SELECT 
        student_id,
        call_closed_date,
        student_grade,
        batch_id,
        session_id,
        start_time_ist,
        pre_rn,
        is_present as pre_is_present_1,
        LEAD(is_present)OVER(partition by student_id, call_closed_date order by pre_rn asc) as pre_is_present_2
    FROM
        PRE
    WHERE pre_rn <= 2
)

,POST_FINAL AS
(
    SELECT 
        student_id,
        call_closed_date,
        student_grade,
        batch_id,
        session_id,
        start_time_ist,
        post_rn,
        is_present as post_is_present_1,
        LEAD(is_present)OVER(partition by student_id, call_closed_date order by post_rn asc) as post_is_present_2
    FROM
        POST
    WHERE post_rn <= 2
)

,PP_FINAL as
(

    SELECT 
        student_id,
        call_closed_date,
        student_grade,
        batch_id,
        session_id,
        start_time_ist,
        lat_post_rn,
        is_present as lat_p_is_present_1,
        LEAD(is_present)OVER(partition by student_id, call_closed_date order by lat_post_rn asc) as lat_p_is_present_2
    FROM
        POST
    WHERE lat_post_rn <= 2
)


,BASE AS
(SELECT
    prf.student_id,
    prf.call_closed_date,
    prf.student_grade,
    pre_is_present_1,
    pre_is_present_2,
    post_is_present_1,
    post_is_present_2,
    lat_p_is_present_1,
    lat_p_is_present_2

FROM
    PRE_FINAL prf
LEFT join
    POST_FINAL psf on prf.student_id = psf.student_id and prf.call_closed_date = psf.call_closed_date and prf.batch_id = psf.batch_id
                    AND pre_rn = 1 and post_rn = 1
LEFT JOIN
    PP_FINAL ppf on prf.student_id = ppf.student_id and prf.call_closed_date = ppf.call_closed_date and prf.batch_id = ppf.batch_id
                    AND lat_post_rn = 1
WHERE
    pre_rn = 1
)



SELECT
    COUNT(distinct student_id) as total_called_students,
----------- 2 days absent criteria
    COUNT(distinct CASE WHEN pre_is_present_1 = 0 AND pre_is_present_2 = 0 THEN student_id END) as streak_absent_called_students,
    COUNT(distinct CASE WHEN pre_is_present_1 = 0 AND pre_is_present_2 = 0 AND post_is_present_1 = 1 AND post_is_present_2 = 1
     THEN student_id END) as streak_absent_streak_activated_students,
     COUNT(distinct CASE WHEN pre_is_present_1 = 0 AND pre_is_present_2 = 0 AND post_is_present_1 = 1 AND post_is_present_2 = 1
     AND lat_p_is_present_1 = 1 AND lat_p_is_present_2 = 1 THEN student_id END) as streak_absent_streak_retained_students,
-----------  10 days criteria
    COUNT(distinct CASE WHEN pre_is_present_1 = 1 OR pre_is_present_2 = 1 THEN student_id END) as other_absent_called_students,
    COUNT(distinct CASE WHEN pre_is_present_1 = 1 OR pre_is_present_2 = 1 AND post_is_present_1 = 1 AND post_is_present_2 = 1
     THEN student_id END) as other_absent_other_activated_students,
     COUNT(distinct CASE WHEN pre_is_present_1 = 1 OR pre_is_present_2 = 1 AND post_is_present_1 = 1 AND post_is_present_2 = 1
     AND lat_p_is_present_1 = 1 AND lat_p_is_present_2 = 1 THEN student_id END) as other_absent_other_retained_students
FROM
    BASE
