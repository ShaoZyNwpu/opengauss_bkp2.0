set enable_global_stats = true;
--SQLONHADOOP-50 
create table create_query_plan_table_031(c_smallint smallint not null,c_double_precision double precision,c_time_without_time_zone time without time zone null,c_time_with_time_zone time with time zone,c_integer integer default 23423,c_bigint bigint default 923423432,c_decimal decimal(19) default 923423423,c_real real,c_numeric numeric(18,12) null,c_varchar varchar(19),c_char char(57) null,c_timestamp_with_timezone timestamp with time zone,c_char2 char default '0',c_text text null,c_varchar2 varchar2(20),c_timestamp_without_timezone timestamp without time zone,c_date date,c_varchar22 varchar2(11621),c_numeric2 numeric null) tablespace hdfs_ts;
select (c_smallint,c_double_precision)>(12100,200) from create_query_plan_table_031 order by 1;
drop table create_query_plan_table_031;

--SQLONHADOOP-48 
create table create_columnar_alter_common_001_3 (col_5 tinyint,col_6 tinyint) tablespace hdfs_ts;
alter table create_columnar_alter_common_001_3 alter column col_6 type money;
drop table create_columnar_alter_common_001_3;

--SQLONHADOOP-49 
create table test_table_1(c_smallint smallint ,c_bytea bytea) tablespace hdfs_ts ;


