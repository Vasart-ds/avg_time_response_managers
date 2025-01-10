with message_sorting as (
select  type,
         entity_id,
         created_by,
         created_at,
         lead(created_at) over (partition by entity_id order by created_at) as manager_answer_time,
         lag(type) over (partition by entity_id order by created_at) as prev_type
 from test.chat_messages
),
filtered_msgs as (
	select entity_id,
           created_by,
           created_at,
           manager_answer_time,
           case
           		when (prev_type is null and lead(prev_type) over (partition by entity_id) = 'incoming_chat_message') then '0'
          		when (prev_type is null and lead(prev_type) over (partition by entity_id) is null) then 'non_answ'
          		when lag(prev_type) over (partition by entity_id) is null then '1'
           else prev_type
           end as upd_type
 	from message_sorting
),
response_time as (
	select entity_id,
 		created_at,
 		manager_answer_time,
 		(case when created_by = 0 then lead(created_by) over (partition by entity_id order by created_at) end) as manager_id,
 		(case when manager_answer_time is not null and created_at is not null then 
                greatest(to_timestamp(manager_answer_time)::time, '09:30:00') -
                greatest(to_timestamp(created_at)::time, '00:00:00') else null end) as response_lag
	from filtered_msgs
	where (upd_type = '0' or upd_type = '1')
),
avg_response as (
select rt.manager_id,
	   m.name_mop,
	   AVG(rt.response_lag::time) as avg_response_time,
	   r.rop_name
from response_time rt
join test.managers m on rt.manager_id = m.mop_id
join test.rops r on r.rop_id = m.rop_id::int
where manager_id is not null and manager_id != 0
group by rt.manager_id, m.name_mop, r.rop_name
)

select manager_id,
	   name_mop,
	   rop_name,
	   avg_response_time::time
	   case 
	   	when extract(hour from avg_response_time::time) > 0 then 
		 	 extract(hour from avg_response_time::time) * 60 + extract(minute from avg_response_time::time)
	   else extract(minute from avg_response_time::time)
	   end as avg_response_minutes
from avg_response
order by avg_response_minutes;