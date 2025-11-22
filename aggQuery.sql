insert into agreements
select correlation_id,
    LISTAGG(schedule_id) as schedule_ids
from onboarding
group by correlation_id
having COUNT(total_parts) >= MAX(total_parts);
