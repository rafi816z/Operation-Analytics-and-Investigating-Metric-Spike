create database project3;
show databases;
use project3;
show variables like 'secure_file_priv';

#create table job_data_final
create table job_data_final(
ds varchar(100),
job_id int,
actor_id int,
event varchar(100),
language varchar(50),
time_spent int,
org varchar(50));


# import job_data_final file into MySQL
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/job_data_final.csv"
into table job_data_final
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


#change type of column ds and update it
alter table job_data_final add column temp_ds datetime;
update job_data_final set temp_ds=str_to_date(ds, '%m/%d/%Y');
alter table job_data_final drop column ds;
alter table job_data_final change column temp_ds ds date;
#______________________________________________________________________________________________________________________________________________________

#create table users
create table users(
user_id int,
created_at varchar(100),
company_id int,	
language varchar(50),
activated_at varchar(100),
state varchar(50));

# import users file into MySQL
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
into table users
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


# changing type of column created_at and activated_at and update it
alter table users add column temp_created_at datetime;
update users set temp_created_at=str_to_date(created_at, '%d-%m-%Y %H:%i');
alter table users drop column created_at;
alter table users change column temp_created_at created_at datetime;

alter table users add column temp_activated_at datetime;
update users set temp_activated_at=str_to_date(activated_at, '%d-%m-%Y %H:%i');
alter table users drop column activated_at;
alter table users change column temp_activated_at activated_at datetime;
#______________________________________________________________________________________________________________________________________________________

# create table email_events
create table email_events(
user_id int,
occurred_at varchar(100),
action varchar(50),
user_type int);


#import email_events file into MySQL
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


# changing type of column occurred_at and update it
alter table email_events add column temp_occurred_at datetime;
update email_events set temp_occurred_at=str_to_date(occurred_at, '%d-%m-%Y %H:%i');
alter table email_events drop column occurred_at;
alter table email_events change column temp_occurred_at occurred_at datetime;
#______________________________________________________________________________________________________________________________________________________

# create table events
create table `events`(
user_id int,
occurred_at varchar(100),
event_type varchar(50),
event_name varchar(50),
location varchar(50),
device varchar(100),
user_type int);


#import events file into MySQL
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table `events`
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


#changing type of column occurred_at and update it
alter table events add column temp_occurred_at datetime;
update events set temp_occurred_at=str_to_date(occurred_at, '%d-%m-%Y %H:%i');
alter table events drop column occurred_at;
alter table events change column temp_occurred_at occurred_at datetime;
select * from events;