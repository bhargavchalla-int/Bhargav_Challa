WITH call_data as 
(
    select
        "Student_Lead_Id",
        "CLM_Query_Type",
        "Status",
        get_timestamp_ist("ClosedDate"::text) as "ClosedDate",
        replace(split_part("Description",':',2),'seconds','') as call_duration,
        "Lead_Grade",
        "GeoGraphy"
    from bhanzu_analytics.dynamo_prod_alltickets
    where 1=1
      and "CLM_Query_Type" = 'absenteeism_call'
      and "Priority" = 'medium'
      and "Status" = 'closed'
      and "Description" <> '..'
      and "GeoGraphy" = 'IND'
      and get_timestamp_ist("ClosedDate"::text) >= '2025-08-01T00:00:00.000Z'
      and get_timestamp_ist("ClosedDate"::text) < '2025-09-01T00:00:00.000Z'
)




Select
"Lead_Grade",
COUNT(DISTINCT "Student_Lead_Id") as students_called,
COUNT(distinct CASE WHEN session_type = 'Batch Session' AND session_status = 'completed' AND is_present = 1 then lead_id END) as present_studs,
COUNT(distinct CASE WHEN session_type = 'Batch Session' AND session_status = 'completed' AND is_present = 0 then lead_id END) as absent_studs,
COUNT(distinct CASE WHEN session_type = 'Batch Session' AND session_status = 'completed' then session_id END) as completed_sessions,
COUNT(distinct CASE WHEN session_type = 'Batch Session' AND session_status = 'completed' AND is_present = 1 then session_id END) as completed_attended_sessions,
COUNT(distinct CASE WHEN session_type = 'Batch Session' AND session_status = 'completed' AND is_present = 0 then session_id END) as completed_skipped_sessions


FROM
(Select
        cd.*,
        sd.*
    from call_data cd
    left join quicksight_mat_views.students_eligible_batch_sessions sd
        on cd."Student_Lead_Id" = sd.lead_id
        and sd.start_time_ist > get_timestamp_ist(cd."ClosedDate"::text)
        and session_status NOT IN ('active','inactive')
) as p

group by 1
order by 1
