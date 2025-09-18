with student_data as 
    (
        select
            profile_id,
            base_lead_id,
            __hevo__source_modified_at,
            login_veil,
            country,
            absolute_grade,
            grade

        from
            bhanzu_analytics.post_login_prod_userprofiles
        where
            profile_type = 'STUDENT'
        
    )
, parents_data as 
    (
        select 
            profile_id,
            base_lead_id,
            __hevo__source_modified_at
        from
            bhanzu_analytics.post_login_prod_userprofiles
        where
            profile_type = 'PARENT'
            and login_veil is not null
    )

, final_details as 
    (
        select
            fid.profile_id,
            fid.base_lead_id,
            fid.__hevo__source_modified_at
        from
            student_data fid
        inner join
            parents_data f 
                    on fid.base_lead_id = f.base_lead_id
    )
, unioned_profiles as 
    (
        select
            profile_id  , __hevo__source_modified_at
        from
            student_data
        where login_veil is not null
        
        union  
        
        select
            profile_id  , __hevo__source_modified_at
        from
            final_details
    )
, Enrolled as 
    (
        select 
            p.variant_id,
            p.profile_id,
            p.product_id,
            p.batch_id,
            p.status,
            up.country,
            up.absolute_grade,
            up.grade
        from
            bhanzu_analytics.prod_enrolledproducts p
        left join
            student_data up on p.profile_id = up.profile_id
        where
            p.status = 'active' 
            and p.batch_id is not null
    )

, unreg_paid as
    (
        select
            distinct e.*  -- get the count for % un-registered
        from
            Enrolled e
        left join
            unioned_profiles u
                    on u.profile_id = e.profile_id
        where u.profile_id is null
    )
, PRE_FINAL_A as
    (
        Select
            up.profile_id as student_id,
            up.batch_id,
            mv.teacher_id,
            up.country,
            up.grade,
            up.absolute_grade,
            MIN(mv.start_time_ist) as batch_start_date
    from
            unreg_paid up
    left join
            quicksight_mat_views.students_eligible_batch_sessions mv on up.profile_id = mv.lead_id
    group by 1,2,3,4,5,6
    )
    Select
        CASE
            WHEN batch_start_date BETWEEN current_date - 90 and current_date THEN 'a.last 3 months'
            WHEN batch_start_date BETWEEN current_date - 180 and current_date - 91 THEN 'b.last 6 months'
            WHEN batch_start_date BETWEEN current_date - 360 and current_date - 181 THEN 'c.last 1 year'
            WHEN batch_start_date < current_date - 360  THEN 'd.past 1 year'
            else 'e.others' -- exception
            END as batch_timeframe,
        count(distinct student_id) as students
    FROM
        PRE_FINAL_A
        group by 1
        order by 1 asc
