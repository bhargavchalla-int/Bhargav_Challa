WITH batches as
(   SELECT
        batch_id,
        name,
        schedule_start_date,
        schedule_end_date,
        batch_status,
        center_name,
        schedule_session_times,
        schedule_session_days,
        num_sessions,
        strength,
        capacity,
        grade,
        ROW_NUMBER()OVER(PARTITION BY batch_id order by __hevo__ingested_at desc) as rn
    FROM
        bhanzu_analytics.prod_batches_oc
)

,batch_sessions_initial as
(
    SELECT
        bs.batch_id,
        bs.teacher_id,
        bs.session_id,
        bs.session_type,
        bs.session_status,
        get_timestamp_ist(bs.Start_time::text) as session_start_time,
        get_timestamp_ist(bs.end_time::text) as session_end_time,
        bs.teacher_join_duration,
        bs.topics as topics,
        bs.module_id,
        bs.module_status,
        bs.comments_category,
        bs.comments,
        bs.teacher_utilization,
        bs.cancelled_at,

        b.name as batch_name,
        b.schedule_start_date as batch_schedule_start_date,
        b.schedule_end_date as batch_schedule_end_date,
        b.batch_status,
        b.center_name as batch_center_name,
        b.schedule_session_times,
        b.schedule_session_days,
        b.num_sessions,
        b.strength as batch_strength,
        b.capacity as batch_capacity,
        b.grade as batch_grade_level
    FROM
        etl_bhanzu_analytics.prod_batchsessions_oc bs
        left join
        batches as b ON bs.batch_id = b.batch_id and b.rn = 1
    WHERE 1=1
        and bs.session_status IN ('completed','cancelled')
        and b.name <> 'LC-MW-TX-C001-B1111(TEST)' -- exclusive test batch
)

, session_topic_data as
 (
        with raw_topics as 
        (
            select 
                session_id,
                module_id,
                json_array_elements(topics) as final_topics
            from batch_sessions_initial
            where topics is not null
        ),
        extracted_topics as 
        (
            select 
                session_id,
                module_id,
                replace(replace(split_part(CAST(final_topics AS TEXT),',',1),'"','') , '[','')  as topic_id,
                replace(replace(split_part(CAST(final_topics AS TEXT),',',2),'"','') , ']','')  as topic_status
            from raw_topics
            group by session_id, module_id, topic_id, topic_status
        ),
        topic_data as
        (
        SELECT
            module_id,
            topic_id,
            topic_name,
            topic_grade
        FROM
            (
            SELECT
                module_id,
                id as topic_id,
                name as topic_name,
                grade as topic_grade,
                row_number() over (PARTITION BY id order by modified_at desc) as row_number
            FROM
                etl_bhanzu_analytics.prod_lmstopic_oc
                where status = 'active'
            ) as f where row_number = 1
    )
    select distinct
        et.session_id,
        et.module_id,
        et.topic_id,
        et.topic_status,
        td.topic_name,
        td.topic_grade
    from extracted_topics et
    left join topic_data td
        on et.topic_id = td.topic_id
)

,batch_sessions as  -- final for use, not above two CTEs
(
    SELECT
        bs.*,
        st.topic_id,
        st.topic_name,
        st.topic_grade,
        st.topic_status,
        MIN(bs.session_start_time) OVER(PARTITION BY batch_id ORDER BY bs.session_start_time asc) as batch_actual_start_date,
        ROW_NUMBER()OVER(PARTITION BY bs.batch_id order by bs.session_start_time asc) as lesson_number
    FROM
        batch_sessions_initial bs
    LEFT JOIN
        session_topic_data st ON bs.session_id = st.session_id
)

,topic_sequence as 
    (
     WITH topic_seq as
        (SELECT
                module_id,
                grade,
                module_name,
                module_status,
                topic_id,
                topic_seq
        FROM
            (SELECT
                    c.id as module_id,
                    c.grade,
                    c.name as module_name,
                    c.status as module_status,
                    t.topic_id,
                    row_number() OVER (PARTITION BY  c.id ORDER BY t.ordinality) AS topic_seq
                FROM
                    etl_bhanzu_analytics.prod_lmsmodule_oc c
                    CROSS JOIN LATERAL jsonb_array_elements_text(c.topic_ids::jsonb) WITH ORDINALITY t(topic_id, ordinality)
                WHERE
                    c.status::text = 'active'::text
            ) as p
            )

    ,module_seq as
            (SELECT
                f.course_code,
                f.module_id,
                f.module_seq
                
            FROM 
                (SELECT 
                    split_part(c.grade,'-',1) as grade,
                    t.module_id,
                    c.batch_type,
                    btrim(lower(c.code::text)) AS course_code,
                    row_number() OVER (PARTITION BY c.grade, c.code, c.batch_type ORDER BY t.ordinality) AS module_seq
                FROM
                    etl_bhanzu_analytics.prod_lmscourse_oc c
                    CROSS JOIN LATERAL jsonb_array_elements_text(c.module_ids::jsonb) WITH ORDINALITY t(module_id, ordinality)
                WHERE
                    c.status::text = 'active'::text
                ) as f
            )

        SELECT
            t.*,
            m.course_code,
            m.module_seq
        FROM
            topic_seq t
        LEFT JOIN
            module_seq m ON t.module_id = m.module_id
        order by module_seq, topic_seq
    )

,trainers as
(
    SELECT DISTINCT
        t.teacher_id,
        t.trainer_name,
        t.gender as trainer_gender,
        -- t.trainer_status,
        t.joined_on as trainer_joined_on
    FROM
        bhanzu_analytics.prod_trainers_oc as t -- trainer working schedule oc
)

,worksheet_scores as
(
    Select
        id as score_id,
        get_timestamp_ist(created_on::text) as created_time_ist,
        case when is_module_flag = '1' then generic_id end as module_id,
        case when is_module_flag = '0' then generic_id end as topic_id,
        profile_id as student_id,
        score,
        total_marks,
        worksheet_id
    from
        etl_bhanzu_analytics.prod_studentscores_oc
)

,student_attendance as
(
    Select DISTINCT
        a.batch_id,
        a.student_id,
        a.session_id,
        CASE WHEN a.is_present = TRUE then 'Yes' else 'No' end as student_attendance,
        a.attendance_given_on,
        s.name as student_name,
        CASE WHEN s.registered = true then 'yes' else 'no' end as is_student_registered
    FROM
        bhanzu_analytics.batchattendance_v2_oc as a
        LEFT JOIN
        etl_bhanzu_analytics.prod_userprofiles_oc s ON a.student_id = s.profile_id AND s.profile_type = 'STUDENT'
)

,teacher_attendance as
(
    SELECT
        batch_id,
        session_id,
        teacher_id,
        session_start_time,
        CASE WHEN lower(comments_category) like '%trainer%' then 'Absent' else 'Present' end as teacher_attendance,
        teacher_join_duration,
        comments_category,
        comments,
        teacher_utilization,
        cancelled_at
    FROM
        batch_sessions_initial
)

,trainer_planned_leaves as
(
    Select
        teacher_id,
        status as leave_status,
        get_timestamp_ist(leave_start) as leave_start_date,
        get_timestamp_ist(leave_end) as leave_end_date
    from
        bhanzu_analytics.trainer_leaves
)

,trainer_cancellations as
(
    SELECT
        teacher_id,
        batch_id,
        count(distinct case when teacher_attendance = 'Absent' then session_id end) as trainer_total_cancellations,
        count(distinct case when teacher_attendance = 'Absent' AND DATE(session_start_time) BETWEEN current_date -15 and current_date-1 then session_id end) as trainer_last2weeks_cancellations

    FROM
        teacher_attendance
        group by teacher_id,batch_id
)


,available_assessment_data as  -- Pre & post assessments
(
    select
         assessment_id
        , module_id
        , 'ASSESSMENT' as test_type
        , assessment_category_type
        , assessment_category_id
        , assessment_topic
        , assessment_category_view_status
    from
         etl_assessments_analytics.assessment_info 
    where
         assessment_publishstatus = 'true'
    and assessment_status = 'true'
    and assessment_type = 'WORKSHEET'
)
,lms_module as
(
    select
          module_id
        , module_name
        , grade
    from (
        SELECT id as module_id
            , name as module_name
            , grade
            , row_number() over (PARTITION BY id order by modified_at desc) as row_number
        FROM etl_bhanzu_analytics.prod_lmsmodule_oc
    ) as f where row_number = 1
)

,available_assessment_module_data as 
(
    select 
        aad.*
        , lm.module_name
        , lm.grade
    from 
        available_assessment_data aad
    left join
        lms_module lm on aad.module_id = lm.module_id
)
,completed_assessment_data as 
(
    select
        user_id as assessment_completed_user_id
        , grade as assessment_completed_grade
        , case when test_type = 'IN-CLASS' then pre_defined_room_id else assessment_id end as assessment_id
        , module_id as assessment_completed_module_id
        , topic_id as assessment_completed_topic_id
        , get_timestamp_ist(timestamp::text) as assessment_completed_timestamp_ist
        , 'ASSESSMENT' as assessment_completed_test_type
    from
        etl_assessments_analytics.assessment_attempt_report
)

SELECT
    --------------------- Student attendance columns
    sa.student_id as "Student Id",
    sa.student_name as "Student Name",
    sa.student_attendance as "Student Attendance Status",
    sa.attendance_given_on as "Student Attendance Date",
    sa.is_student_registered as "Is Student Registered^",

    --------------------- batch session columns
    bs.teacher_id as "Teacher Id",
    bs.session_type as "Session Type",
    bs.session_status as "Session Status",
    bs.session_id as "Session Id",
    bs.batch_id as "Batch Id",
    bs.session_start_time as "Lesson Start Date", --- naming it as lesson start date instead of session start date
    bs.session_end_time as "Session End date",
    bs.teacher_join_duration as "Teacher Join Duration",
    bs.topic_id as "Lesson Id",
    bs.topic_status as "Lesson Status",
    bs.topic_name as "Lesson Name",
    bs.lesson_number      as "Lesson Actual Seq",
    bs.module_id as "Module Id",
    bs.module_status as "Module Status",
    bs.comments_category  as "Session Comments Category",
    bs.comments           as "Session Comments",
    bs.teacher_utilization as "Teacher Utilization",
    bs.cancelled_at       as "Session Cancelled time",
    bs.batch_name         as "Batch Name",
    bs.batch_schedule_start_date as "Batch Scheduled Start Date",
    bs.batch_actual_start_date as "Batch Start Date",
    bs.batch_schedule_end_date as "Batch End Date",
    bs.batch_status       as "Batch Status",
    bs.batch_center_name as "Batch Center",
    bs.schedule_session_times as "Schedule Session Times",
    bs.schedule_session_days as "Schedule Session Days",
    bs.num_sessions as "Number of Sessions plan",
    bs.batch_strength as "Batch Strength",
    bs.batch_capacity as "Batch Capacity",
    split_part(bs.batch_grade_level,'-',1) as "Batch Grade",
    ----------------------- topics and module sequence
    CASE WHEN ws.worksheet_id is not null then 'yes' else 'no' end as "Worksheet Evaluated",
    ROUND((ws.score/ws.total_marks)*100,0) as "Worksheet Score",
    seq.topic_seq as "Lesson Scheduled Seq",
    seq.module_seq as "Module Scheduled Seq",
    lm.module_name as "Module Name",
    -----------------------  trainer columns
    t.trainer_name as "Trainer Name",
    t.trainer_gender as "Trainer Gender",
    t.trainer_joined_on as "Trainer Joined Date",
    
    tc.trainer_total_cancellations as "Trainer Total Cancellations",
    tc.trainer_last2weeks_cancellations as "Trainer Last2weeks Cancellations",

    ta.teacher_attendance as "Teacher Attendance Status",
    ta.cancelled_at as "Trainer Cancelled At",
    tpl.leave_status as "Teacher Leave Status" ,-- not for use

    -----------------------   assessment available columns
    aamd.assessment_id as "Available Assessment Id",
    aamd.module_id as "Available Module Id",
    aamd.module_name as "Available Assessment Module Name",
    CONCAT(aamd.assessment_category_view_status, '-', aamd.assessment_category_type) as "Available Assessment Type",
    aamd.assessment_topic as "Assessment Lesson Name",
    

-----------------------   assessment Completed columns

    cad.assessment_id as "Completed Assessment Id",
    cad.assessment_completed_grade as "Completed Assessment Student Grade",
    cad.assessment_completed_module_id as "completed Assessment Module Id",
    cad.assessment_completed_topic_id as "completed Assessment Lesson Id",
----------------------- last refreshed time ist
    get_timestamp_ist(current_timestamp::text) as last_refreshed_at_ist

-- dont use columns "student_registered", "assessments related"

FROM
    batch_sessions bs
LEFT JOIN
    student_attendance sa ON bs.session_id = sa.session_id
LEFT JOIN
    teacher_attendance ta ON bs.session_id = ta.session_id
LEFT JOIN
    trainers t ON bs.teacher_id = t.teacher_id
LEFT JOIN
    available_assessment_module_data aamd on bs.module_id = aamd.module_id
LEFT JOIN
    completed_assessment_data cad on aamd.assessment_id = cad.assessment_id and sa.student_id = cad.assessment_completed_user_id
LEFT JOIN
    trainer_cancellations tc on bs.teacher_id = tc.teacher_id and bs.batch_id = tc.batch_id
LEFT JOIN
    topic_sequence as seq ON bs.module_id = seq.module_id and bs.topic_id = seq.topic_id
LEFT JOIN
    lms_module as lm ON bs.module_id = lm.module_id
LEFT JOIN
    worksheet_scores as ws ON sa.student_id = ws.student_id AND bs.topic_id = ws.topic_id
LEFT JOIN
    trainer_planned_leaves tpl on bs.teacher_id = tpl.teacher_id AND bs.session_start_time BETWEEN tpl.leave_start_date and tpl.leave_end_date
