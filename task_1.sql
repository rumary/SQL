SELECT new_date,
       COUNT(global_session_id) number_of_session
FROM(
    SELECT  *,
            date_trunc('hour', min_date) as new_date
    FROM(
            SELECT user_id,
                   session_id,
                   global_session_id,
                   min(min_date) min_date,
                   max(max_date) max_date
            FROM(
                SELECT distinct *,
                       max(page_rnk) over(partition by user_id)  unq_pages
                FROM(
                    SELECT user_id, session_id, global_session_id, right_action_sequence,
                               min(happened_at) OVER (PARTITION BY user_id) AS min_date, 
                               max(happened_at) OVER (PARTITION BY user_id) AS max_date,
                               dense_rank() over(partition by user_id order by page)  page_rnk
                    FROM(
                            SELECT *,
                                sum(is_new_session) over(partition by user_id order by happened_at rows between unbounded preceding and current row) AS session_id,
                                sum(page_up) over(partition by user_id order by happened_at rows between unbounded preceding and current row) AS right_action_sequence,
                                sum(is_new_session) OVER (ORDER BY user_id, happened_at rows between unbounded preceding and current row) AS global_session_id
                            FROM(
                                    SELECT *,
                                          CASE WHEN EXTRACT('EPOCH' FROM happened_at) - EXTRACT('EPOCH' FROM last_event) >= (3600) 
                                                OR last_event IS NULL THEN 1 ELSE 0 END AS is_new_session,
                                          CASE WHEN LAG(page) over(partition by user_id order by happened_at) > page then 1 else 0 end page_up
                                     FROM(
                                            SELECT user_id,
                                                   happened_at,
                                                   CASE page WHEN 'rooms.homework-showcase' then '1' 
                                                             WHEN 'rooms.view.step.content' then '2'
                                                             WHEN 'rooms.lesson.rev.step.content' then '3'
                                                             ELSE '0' 
                                                             END AS PAGE,
                                                   LAG(happened_at,1) OVER (PARTITION BY user_id ORDER BY happened_at) AS last_event
                                            FROM test.vimbox_pages
                                          )
                                )
                        )
                    )
            )
            GROUP BY user_id, session_id, global_session_id
            HAVING max(unq_pages) >= 3
        )
    )
GROUP BY new_date
