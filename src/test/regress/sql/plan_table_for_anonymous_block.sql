drop table if exists t_s_condition_0011_1 cascade;
drop table if exists t_s_condition_0011_2;
create table t_s_condition_0011_2(id int) with (storage_type=ustore);
insert into t_s_condition_0011_2 values(1);

create table t_s_condition_0011_1(staff_id int not null, highest_degree char(8), graduate_school varchar(64), graduate_date smalldatetime, t_education_note varchar(70)) with (storage_type=ustore);
insert into t_s_condition_0011_1(staff_id,highest_degree,graduate_school,graduate_date,t_education_note) values(10,'博士','西安电子科技大学','2017-07-06 12:00:00','211');
insert into t_s_condition_0011_1(staff_id,highest_degree,graduate_school,graduate_date,t_education_note) values(11,'博士','西北农林科技大学','2017-07-06 12:00:00','211和985');
insert into t_s_condition_0011_1(staff_id,highest_degree,graduate_school,graduate_date,t_education_note) values(12,'硕士','西北工业大学','2017-07-06 12:00:00','211和985');
insert into t_s_condition_0011_1(staff_id,highest_degree,graduate_school,graduate_date,t_education_note) values(15,'学士','西安建筑科技大学','2017-07-06 12:00:00','非211和985');
insert into t_s_condition_0011_1(staff_id,highest_degree,graduate_school,graduate_date,t_education_note) values(18,'硕士','西安理工大学','2017-07-06 12:00:00','非211和985');
insert into t_s_condition_0011_1(staff_id,highest_degree,graduate_school,graduate_date,t_education_note) values(20,'学士','北京师范大学','2017-07-06 12:00:00','211和985');

create or replace function f_s_condition_0011(a int) return int
as
c int;
d int;
begin
c := a;
select id into d from t_s_condition_0011_2 where rownum = c;
return d;
end ;
/

drop procedure if exists p_s_condition_0011_1;
create or replace procedure p_s_condition_0011_1
as
begin
explain plan for select * from (select highest_degree,graduate_date from t_s_condition_0011_2 right join t_s_condition_0011_1 on staff_id = id where f_s_condition_0011(1) = 1) where graduate_date < to_date('2018-06-28 13:14:15', 'yyyy-mm-dd hh24:mi:ss:ff') order by highest_degree asc;
explain plan for select * from (select highest_degree,graduate_date from t_s_condition_0011_2 right join t_s_condition_0011_1 on staff_id = id where f_s_condition_0011(1) = 1) where graduate_date < to_date('2018-06-28 13:14:15', 'yyyy-mm-dd hh24:mi:ss:ff') order by highest_degree desc;
end;
/
select p_s_condition_0011_1();

create or replace procedure t_ustore_Proc_temp_table_0008(v_name varchar2) as
v_num int;
begin
execute immediate 'drop table if exists t_ustore_Proc_temp_table_'||v_name;
execute immediate 'create local temporary table t_ustore_Proc_temp_table_'||v_name||'(id int,name varchar2(200)) with (storage_type=ustore)';
end;
/

declare
v_name varchar2:= '0008';
begin
t_ustore_Proc_temp_table_0008(v_name);
end;
/
