--todo:
--constraint test case
--04 cascade semantics test,
--08 test the fk constraint
--19 truncate partition with fk/pk constraint


--01 syntax test: "table" key word test
--02 truncate multi table
--03 'restart identity' key word test
--04 cascade semantics test,  
--05 cascade test
--06 permission test
--07 --partitioned table has a local partitioned index ,when the partitioned table was truncate 
--the associated index should be truncate too.
--08 test the fk constraint
--09 test the truncate with in a transaction or truncate in diff transaction
--10 test the before truncate trigger   and after truncate trigger
--11 test a table with toast table
--12 test the interval partitioned table be truncated
--13 after add partition /drop partition, truncate table
--14 sytax test,   missing partition key word
--15 test with index add/drop partition, toast table, transaction
--16 partitioned table has toast table partition
--17 truncate command and create table in same transaction
--18 truncate partiton and drop parttion in same transaction
--19 truncate partition with fk/pk constraint
--20 partiton for multy column
--21 add partition, create a new interval partition,truncate in same transsaction
--22 truncate same partition in a command
--23 test for truncate parititon for null, maxvalue

--01--------------------------------------------------------------------
--"table" key word test
create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);
insert into partition_truncate_table select generate_series(0,100), generate_series(0,100);

select count(*) from partition_truncate_table;
--101 rows
truncate table partition_truncate_table;
select count(*) from partition_truncate_table;
--0 rows
drop table partition_truncate_table;

create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);
insert into partition_truncate_table select generate_series(0,100), generate_series(0,100);

select count(*) from partition_truncate_table;
--101 rows
truncate  partition_truncate_table;
select count(*) from partition_truncate_table;
--0 rows
drop table partition_truncate_table;

--02--------------------------------------------------------------------
--truncate multi table
create table partition_truncate_table1
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table1_p0 values less than (50),
	partition partition_truncate_table1_p1 values less than (100),
	partition partition_truncate_table1_p2 values less than (150)
);
create table partition_truncate_table2
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table2_p0 values less than (50),
	partition partition_truncate_table2_p1 values less than (100),
	partition partition_truncate_table2_p2 values less than (150)
);
insert into partition_truncate_table1 select generate_series(0,100), generate_series(0,100);
insert into partition_truncate_table2 select generate_series(0,100), generate_series(0,100);
select count(*) from partition_truncate_table1;
-- 101 rows
select count(*) from partition_truncate_table2;
-- 101 rows
truncate table partition_truncate_table1,partition_truncate_table2;
select count(*) from partition_truncate_table1;
--0 rows
select count(*) from partition_truncate_table2;
--0 rows
drop table partition_truncate_table1, partition_truncate_table2;



create table partition_truncate_table1
(
	c1 int,
	c2 int
);

create table partition_truncate_table2
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table2_p0 values less than (50),
	partition partition_truncate_table2_p1 values less than (100),
	partition partition_truncate_table2_p2 values less than (150)
);
insert into partition_truncate_table1 select generate_series(0,100), generate_series(0,100);
insert into partition_truncate_table2 select generate_series(0,100), generate_series(0,100);
select count(*) from partition_truncate_table1;
-- 101 rows
select count(*) from partition_truncate_table2;
-- 101 rows
truncate table partition_truncate_table1,partition_truncate_table2;
select count(*) from partition_truncate_table1;
--0 rows
select count(*) from partition_truncate_table2;
--0 rows

drop table partition_truncate_table1, partition_truncate_table2;

--03--------------------------------------------------------------------
--'restart identity' key word test

--default ,no restart
create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c2)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);


insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
select * from partition_truncate_table order by 1, 2;
--3 rows
truncate partition_truncate_table;

insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
select * from partition_truncate_table order by 1, 2;
--no restart 4 5 6
drop table partition_truncate_table;

--restart 
create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c2)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);


insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
select * from partition_truncate_table order by 1, 2;
--3 rows
truncate partition_truncate_table restart identity;

insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
select * from partition_truncate_table order by 1, 2;
--restart 1 2 3
drop table partition_truncate_table;




--
create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c2)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);


insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
select * from partition_truncate_table order by 1, 2;
--3 rows
truncate partition_truncate_table continue identity;

insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
insert into partition_truncate_table values (default, 100);
select * from partition_truncate_table order by 1, 2;
--no restart 4 5 6
drop table partition_truncate_table;

--04--------------------------------------------------------------------
--cascade semantics test
--non partitioned table has fk on partitioned table
--partitioned table has fk on non partitioned table 

create table partition_truncate_table1
(
	c1 int unique,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table1_p0 values less than (50),
	partition partition_truncate_table1_p1 values less than (100),
	partition partition_truncate_table1_p2 values less than (150)
);


create table partition_truncate_table2 
(
	c1 int references partition_truncate_table1(c1),
	c2 int
);

--insert into partition_truncate_table1 values (1,200);
--insert into partition_truncate_table1 values (2,200);
--insert into partition_truncate_table2 values (2,200);
--insert into partition_truncate_table2 values (2,200);
--
--
--truncate table partition_truncate_table1 restrict;
----can't truncate table partition_truncate_table1
--truncate table partition_truncate_table1 cascade ;
--
--select * from partition_truncate_table1;
----0 rows
--select * from partition_truncate_table2;
----0 rows

drop table if exists partition_truncate_table1,partition_truncate_table2;

create table partition_truncate_table1
(
	c1 int unique,
	c2 int
);



create table partition_truncate_table2 
(
	c1 int references partition_truncate_table1(c1),
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table2_p0 values less than (50),
	partition partition_truncate_table2_p1 values less than (100),
	partition partition_truncate_table2_p2 values less than (150)
);

insert into partition_truncate_table1 values (1,200);
insert into partition_truncate_table1 values (2,200);
insert into partition_truncate_table2 values (2,200);
insert into partition_truncate_table2 values (2,200);

truncate table partition_truncate_table1 restrict;
--can't truncate table partition_truncate_table1
truncate table partition_truncate_table1 cascade ;

select * from partition_truncate_table1 order by 1, 2;
--0 rows
select * from partition_truncate_table2 order by 1, 2;
--0 rows
drop table if exists partition_truncate_table1,partition_truncate_table2;


--05--------------------------------------------------------------------
--cascade test
--partitioned table has fk on a partitioned table

--create table partition_truncate_table1
--(
--	c1 int unique,
--	c2 int
--)
--partition by range (c1)
--interval (10)
--(
--	partition p0 values less than (50),
--	partition p1 values less than (100),
--	partition p2 values less than (150)
--);
--create table partition_truncate_table2 
--(
--	c1 int references partition_truncate_table1(c1),
--	c2 int
--);

--insert into partition_truncate_table1 values (1,200);
--insert into partition_truncate_table1 values (2,200);
--insert into partition_truncate_table2 values (2,200);
--insert into partition_truncate_table2 values (2,200);
--truncate table partition_truncate_table1 ;

----can't truncate table partition_truncate_table1
--drop table if exists partition_truncate_table1,partition_truncate_table2;

--06--------------------------------------------------------------------
--permission
--alter sequence permission and truncate table permission test
--in postgres only the owner can restart the sequence

create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c2)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);


create role partition_truncate_user1 login password 'gauss@123';
grant truncate on table partition_truncate_table to partition_truncate_user1;
set session authorization partition_truncate_user1 password 'gauss@123';
truncate partition_truncate_table;
--truncate table succeed
alter table partition_truncate_table truncate partition partition_truncate_table_p1;
--fail ,not the owner


reset session authorization;
revoke truncate on table partition_truncate_table from partition_truncate_user1;
set session authorization partition_truncate_user1 password 'gauss@123';
truncate partition_truncate_table;
--permission de

reset session authorization;
drop table partition_truncate_table;
drop role partition_truncate_user1;

create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c2)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);

create role partition_truncate_user1 login password 'gauss@123';

grant truncate on table partition_truncate_table to partition_truncate_user1;
grant all on sequence partition_truncate_table_c1_seq to partition_truncate_user1;

set session authorization partition_truncate_user1 password 'gauss@123';

truncate partition_truncate_table restart identity;
--fail, must be the owner of the sequence
reset session authorization;
drop table partition_truncate_table;
drop role partition_truncate_user1;



create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c2)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);

create role partition_truncate_user1 login password 'gauss@123';

grant truncate on table partition_truncate_table to partition_truncate_user1;
grant all on sequence partition_truncate_table_c1_seq to partition_truncate_user1;

set session authorization partition_truncate_user1 password 'gauss@123';

truncate partition_truncate_table ;
--succeed
reset session authorization;
drop table partition_truncate_table;
drop role partition_truncate_user1;


--revoke the truncate permission ,to truncate partitionned table ,and truncate partition
create user partition_truncate_user1 login password 'gauss@123';
set session authorization partition_truncate_user1 password 'gauss@123';
create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c2)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);
reset session authorization;
revoke truncate on table partition_truncate_user1.partition_truncate_table from partition_truncate_user1;
set session authorization partition_truncate_user1 password 'gauss@123';
truncate partition_truncate_table;
--fail , no permission

alter table partition_truncate_table truncate partition partition_truncate_table_p0;
--fail , no permission
alter table partition_truncate_table truncate partition for (0);
--fail , no permission
drop table partition_truncate_table;
reset session authorization;
drop user partition_truncate_user1;
--07--------------------------------------------------------------------
--partitioned table has a local partitioned index ,when the partitioned table was truncate 
--the associated index should be truncate too.

create table partition_truncate_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);

insert into partition_truncate_table  select generate_series(0,100), generate_series(0,100);

create index partition_truncate_table_index on partition_truncate_table(c1) local;
select relname ,reltuples from pg_class where relname = 'partition_truncate_table_index'; 

--analyze partition_truncate_table;
--now unsupport ayalyze partitioned table
truncate partition_truncate_table;
--analyze partition_truncate_table;
--now unsupport ayalyze partitioned table
select relname ,reltuples from pg_class where relname = 'partition_truncate_table_index'; 
-- 0 tuples

insert into partition_truncate_table select generate_series(0,100), generate_series(0,100);

select count(*) from partition_truncate_table;
--101 rows
drop table partition_truncate_table;
--08--------------------------------------------------------------------
-- test the fk constraint
--unsupport the unique constraint

--create table partition_truncate_table1
--(
--	c1 int unique,
--	c2 int
--)
--partition by range (c1)
--interval (10)
--(
--	partition p0 values less than (50),
--	partition p1 values less than (100),
--	partition p2 values less than (150)
--);

--create index partition_truncate_table1_index on partition_truncate_table1(c2) local;
--create table partition_truncate_table2
--(
--	c1 int references partition_truncate_table1(c1) ,
--	c2 int
--)
--partition by range (c1)
--interval (10)
--(
--	partition p0 values less than (50),
--	partition p1 values less than (100),
--	partition p2 values less than (150)
--);

--create index partition_truncate_table2_index on partition_truncate_table2(c2) local;
--insert into partition_truncate_table1  select generate_series(0,100), generate_series(0,100);
--insert into partition_truncate_table2 select generate_series(0,100), generate_series(0,100);

--truncate partition_truncate_table1 cascade;
--select relname ,reltuples from pg_class where relname = 'partition_truncate_table1_index'; 
--select relname ,reltuples from pg_class where relname = 'partition_truncate_table2_index'; 
--drop table partition_truncate_table1,partition_truncate_table2;

--09--------------------------------------------------------------------
--test the truncate with in a transaction or truncate in diff transaction

start transaction;
create table partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);

insert into partition_truncate_table select generate_series(0,100), generate_series(0,100);
select count(*) from partition_truncate_table;
truncate partition_truncate_table;
rollback;
select count(*) from partition_truncate_table;
--can not find the table

create table partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);
start transaction;

insert into partition_truncate_table select generate_series(0,100), generate_series(0,100);
select count(*) from partition_truncate_table;
-- 101 rows
truncate partition_truncate_table;
rollback;
select count(*) from partition_truncate_table;
--0 rows
drop table partition_truncate_table;


--10--------------------------------------------------------------------
--test the before truncate trigger   and after truncate trigger
create table partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);

insert into partition_truncate_table select generate_series(0,100), generate_series(0,100);

create table t1
(
	c1 text
);

create function truncate_trigger() returns trigger as $truncate_trigger$
    begin
       insert into t1 values ('after trigger execute');
        return null ;
    end;
$truncate_trigger$ language plpgsql;

create trigger truncate_partition_trigger after truncate on partition_truncate_table
 execute procedure truncate_trigger();


truncate partition_truncate_table;

select * from t1;
--return 1 rows    'after trigger execute'
drop trigger truncate_partition_trigger on partition_truncate_table ;
drop function truncate_trigger();
drop table partition_truncate_table,t1;

create table partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);

insert into partition_truncate_table select generate_series(0,100), generate_series(0,100);

create table t1
(
	c1 text
);

create function truncate_trigger() returns trigger as $truncate_trigger$
    begin
       insert into t1 values ('before trigger execute');
        return null ;
    end;
$truncate_trigger$ language plpgsql;

create trigger truncate_partition_trigger before truncate on partition_truncate_table
 execute procedure truncate_trigger();


truncate partition_truncate_table;

select * from t1;
--return 1 rows     'before trigger execute'

drop trigger truncate_partition_trigger on partition_truncate_table ;
drop function truncate_trigger();
drop table partition_truncate_table,t1;

--11--------------------------------------------------------------------

--if a partitioned table has varlen column ,each partition of partitioned table must has a 
--associated toast table ,when the partitioned table was truncate ,all the toast table should 
--be truncated
start transaction ;

create table partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);

insert into  partition_truncate_table select generate_series(0,100), 'asdffffffffffffffffffffffffffffffffffffffff';

create index partition_truncate_table_index on partition_truncate_table(c2) local;
select count(*) from partition_truncate_table;
--101 rows
select ct.reltuples,  c.relname from pg_class c , pg_class ct, pg_partition p
where c.relname = 'partition_truncate_table' and p.parentid = c.oid and ct.oid = p.reltoastrelid;

truncate partition_truncate_table;

commit;

select count(*) from partition_truncate_table;
--0 rows
drop table partition_truncate_table;



start transaction ;

create table partition_truncate_table
(
	c1 int ,
	c2 text
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);

insert into  partition_truncate_table select generate_series(0,100), 'asdffffffffffffffffffffffffffffffffffffffff';

create index partition_truncate_table_index on partition_truncate_table(c2) local;
select count(*) from partition_truncate_table;
--101 rows
select ct.reltuples,  c.relname from pg_class c , pg_class ct, pg_partition p
where c.relname = 'partition_truncate_table' and p.parentid = c.oid and ct.oid = p.reltoastrelid;
truncate partition_truncate_table;
rollback;

select count(*) from partition_truncate_table;
--table do not exist
drop table partition_truncate_table;

--12--------------------------------------------------------------------
--test the interval partitioned table be truncated
create table partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (15000)
);

insert into partition_truncate_table values (49,1);
insert into partition_truncate_table values (99,1);
insert into partition_truncate_table values (149,1);
insert into partition_truncate_table values (179,1);
insert into partition_truncate_table values (999,1);
insert into partition_truncate_table values (9999,1);

create index partition_truncate_table_index on partition_truncate_table(c2) local;

select count(*) from partition_truncate_table;
--6 rows

truncate table partition_truncate_table;
select count(*) from partition_truncate_table;
--0 rows
drop table partition_truncate_table;


--13--------------------------------------------------------------------
--after add partition /drop partition, truncate table
create table partition_truncate_table
(
	c1 int ,
	c2 int
)
partition by range (c1)
(
	partition partition_truncate_table_p0 values less than (50),
	partition partition_truncate_table_p1 values less than (100),
	partition partition_truncate_table_p2 values less than (150)
);
insert into partition_truncate_table select generate_series(0,100), generate_series(0,100);

alter table partition_truncate_table add partition partition_truncate_table_p3 values less than (400);
insert into partition_truncate_table select generate_series(300,399), 0;
alter table partition_truncate_table add partition partition_truncate_table_p4 values less than (500);
insert into partition_truncate_table select generate_series(400,499), 0;

select count(*) from partition_truncate_table;
-- 301 rows


truncate table partition_truncate_table;
insert into partition_truncate_table select generate_series(0,499);
select count(*) from partition_truncate_table;
--500 rows

alter table partition_truncate_table drop partition for (300);
select count(*) from partition_truncate_table;
--250 rows
drop table partition_truncate_table;
