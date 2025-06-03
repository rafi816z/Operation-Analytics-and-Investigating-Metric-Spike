use project3;
#to calculate the number of jobs reviewed per hour for each day in November 2020.
select ds as date, count(job_id) as job_reviewed,
count(job_id)/(24) as job_reviewed_per_hour
from job_data_final
where ds between '2020/11/01' and '2020/11/30'
group by ds
order by ds asc;
#________________________________________________________________________________________________________________________________________________________

#Write an SQL query to calculate the 7-day rolling average of throughput
with daily_throughput as(
select ds as date, count(event) as total_event,
(count(event)/86400) as event_per_sec
from job_data_final
group by ds)
select date,total_event,event_per_sec, avg(event_per_sec) over(
order by date
rows between 6 preceding and current row) as 7day_rolling_average
from daily_throughput
group by date
order by date
limit 20;
#________________________________________________________________________________________________________________________________________________________

#calculate the percentage share of each language over the last 30 days.
select language, count(job_id) as total_job,
concat((count(job_id)/(select count(language) 
from job_data_final 
where ds>=date_sub(
(select max(ds) from job_data_final), interval 30 day)))*100,"%") as percentage_share
from job_data_final
where ds>=date_sub((select max(ds) from job_data_final), interval 30 day)
group by language;
#if you want to calculate 30 days percentage from current date then
select language, count(job_id) as total_job,
concat((count(job_id)/(select count(language) 
from job_data_final 
where ds>=date_sub(
curdate(), interval 30 day)))*100,"%") as percentage_share
from job_data_final
where ds>=date_sub(curdate(), interval 30 day)
group by language;
#________________________________________________________________________________________________________________________________________________________

# to find duplicate rows from job_data_final table
SELECT ds, job_id, language, event,actor_id, org, 
COUNT(*) AS duplicate_count
FROM job_data_final
GROUP BY ds, job_id, language, event, actor_id, org
HAVING COUNT(*) > 1;
#________________________________________________________________________________________________________________________________________________________

#Measure the activeness of users on a weekly basis.
SELECT YEAR(occurred_at) AS event_year,
WEEK(occurred_at,1) AS event_week,
COUNT(DISTINCT user_id) AS active_users
FROM events
GROUP BY event_year, event_week
ORDER BY event_year, event_week;

#________________________________________________________________________________________________________________________________________________________

#Write an SQL query to calculate the user growth for the product.
with users_data as(
select date(created_at) as date, count(user_id) as user_registered
from users
group by date(created_at))
select date, user_registered,
sum(user_registered) over(
order by date
) as cumulative_sum
from users_data;
#________________________________________________________________________________________________________________________________________________________

#Write an SQL query to calculate the weekly engagement per device.
select year(users.activated_at) year, week(users.activated_at,1) week, 
events.device, count(users.user_id) active_users from users
right join events
on users.user_id=events.user_id
group by events.device, year, week
order by year asc, week asc, active_users asc;
#________________________________________________________________________________________________________________________________________________________

WITH signup_cohort AS (
SELECT user_id, YEARWEEK(created_at, 1) AS signup_week
FROM users
),
weekly_activity AS (
SELECT user_id, YEARWEEK(occurred_at, 1) AS activity_week
FROM events
)
SELECT sc.signup_week, wa.activity_week,
COUNT(DISTINCT wa.user_id) AS retained_users,
COUNT(sc.user_id) AS cohort_size,
ROUND((COUNT(DISTINCT wa.user_id) * 100.0) / COUNT(sc.user_id), 2) AS retention_rate
FROM signup_cohort sc
LEFT JOIN weekly_activity wa 
ON sc.user_id = wa.user_id AND wa.activity_week >= sc.signup_week
GROUP BY sc.signup_week, wa.activity_week
ORDER BY sc.signup_week, wa.activity_week;
#________________________________________________________________________________________________________________________________________________________

#Write an SQL query to calculate the email engagement metrics.
select  action, count(user_id) no_of_events,
round(count(user_id)*100/(select count(user_id) from email_events),2) as percentage_share 
from email_events
group by action;