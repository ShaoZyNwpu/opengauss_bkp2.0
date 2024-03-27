

--1transcation、create partitioned table----------------------------------------------------------------------
start transaction;

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);
commit;
select c.relname, p.relname from pg_partition p, pg_class c  where  (c.relname = 'cross_test_table' )
			and (p.parentid = c.oid) order by 1, 2;


drop table     cross_test_table;







start transaction;

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);
rollback;
select c.relname, p.relname from pg_partition p, pg_class c  where  (c.relname = 'cross_test_table' )
			and (p.parentid = c.oid) order by 1, 2;


drop table     cross_test_table;
--ERROR:  table "CROSS_TEST_TABLE" does not exist

--transcation、create partiitoned table----------------------------------------------------------------------end




--2transaction create index----------------------------------------------------------------------

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);

start transaction;

create index cross_test_table_index on cross_test_table(c1) local;

commit;

select p.relname , c.relname  from pg_partition p, pg_class c 
				where c.relname  = 'cross_test_table_index' and  p.parentid = c.oid order by 1, 2;

drop table     cross_test_table;





create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);

start transaction;

create index cross_test_table_index on cross_test_table(c1) local;

rollback;

select p.relname , c.relname  from pg_partition p, pg_class c 
				where c.relname  = 'cross_test_table_index' and  p.parentid = c.oid order by 1, 2;
-- RELNAME | RELNAME
-----------+---------
--(0 rows)

drop table     cross_test_table;



--transaction create index----------------------------------------------------------------------end






--3insert value、  create index   、transaction----------------------------------------------------------------------
create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);


create index cross_test_table_index on cross_test_table(c1) local;
insert into cross_test_table values(50, 100),(99,200),(100,200),(101,100),(149,200);
--update cross_test_table set c1 = (c1 +50) where c1 < 100;
delete from cross_test_table where c1 = 149;
select * from cross_test_table order by 1;

drop table     cross_test_table;


start transaction;
create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);


insert into cross_test_table values(50, 100),(99,200),(100,200),(101,100),(149,200);
create index cross_test_table_index on cross_test_table(c1) local;
--update cross_test_table set c1 = (c1 +50) where c1 < 100;
delete from cross_test_table where c1 = 149;

rollback;
select * from cross_test_table order by 1;
--table not exist
select p.relname , c.relname  from pg_partition p, pg_class c 
				where c.relname  = 'cross_test_table_index' and  p.parentid = c.oid order by 1, 2;
-- 0 rows

drop table     cross_test_table;
--table not exist


--insert value、  create index   、transaction----------------------------------------------------------------------end




--4add partition  drop partition  transaction ----------------------------------------------------------------------

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);

start transaction;
alter table cross_test_table add partition p3_cross_test_table values less than (300);
with partitioned_obj_oids as
(
	select oid from pg_class where relname = 'cross_test_table'
)
select p.relname from pg_partition p where p.relname='p3_cross_test_table' and parentid in (select oid from partitioned_obj_oids) order by 1;
commit;
with partitioned_obj_oids as
(
	select oid from pg_class where relname = 'cross_test_table'
)
select p.relname from pg_partition p where p.relname='p3_cross_test_table' and parentid in (select oid from partitioned_obj_oids) order by 1;
drop table     cross_test_table;


create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);

start transaction;
alter table cross_test_table add partition p3_cross_test_table values less than (300);
with partitioned_obj_oids as
(
	select oid from pg_class where relname = 'cross_test_table'
)
select p.relname from pg_partition p where p.relname='p3_cross_test_table' and parentid in (select oid from partitioned_obj_oids) order by 1;
rollback;
with partitioned_obj_oids as
(
	select oid from pg_class where relname = 'cross_test_table'
)
select p.relname from pg_partition p where p.relname='p3_cross_test_table' and parentid in (select oid from partitioned_obj_oids) order by 1;
--0 rows
drop table     cross_test_table;



create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);

start transaction;
alter table cross_test_table drop partition p2_cross_test_table;
select p.relname from pg_partition p where p.relname='p2_cross_test_table' order by 1;
commit;
select p.relname from pg_partition p where p.relname='p2_cross_test_table' order by 1;
drop table     cross_test_table;

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);

start transaction;
alter table cross_test_table drop partition p2_cross_test_table;
select p.relname from pg_partition p where p.relname='p2_cross_test_table' order by 1;
rollback;
select p.relname from pg_partition p where p.relname='p2_cross_test_table' order by 1;
drop table     cross_test_table;
--1 rows
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
--add partition  drop partition  transaction ----------------------------------------------------------------------end


--5add partition  ,drop partition  create index ----------------------------------------------------------------------

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);
alter table cross_test_table add partition p3_cross_test_table values less than (300);
create index cross_test_table_index on cross_test_table(c1) local;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
alter table cross_test_table drop partition p3_cross_test_table ;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
drop table     cross_test_table;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);
alter table cross_test_table drop partition p2_cross_test_table ;
create index cross_test_table_index on cross_test_table(c1) local;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
alter table cross_test_table add partition p3_cross_test_table values less than (300);
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
drop table     cross_test_table;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
--add partition  ,drop partition  create index ----------------------------------------------------------------------end 


--9 drop partition insert values  delete value create index drop index --------------------------------------
create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);
create index cross_test_table_index on cross_test_table(c1) local;
insert into cross_test_table values(50, 100),(99,200),(20,200),(30,100),(146,200);
select * from cross_test_table order by 1;
delete from cross_test_table where c1 = 50;
select * from cross_test_table order by 1;
alter table cross_test_table drop partition p0_cross_test_table;
select * from cross_test_table order by 1;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
drop index cross_test_table_index;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
drop table     cross_test_table;

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);
alter table cross_test_table drop partition p1_cross_test_table;
insert into cross_test_table values(50, 100),(99,200),(20,200),(30,100),(146,200);
select * from cross_test_table order by 1;
delete from cross_test_table where c1 = 50;
create index cross_test_table_index on cross_test_table(c1) local;
select * from cross_test_table order by 1;
alter table cross_test_table drop partition p0_cross_test_table;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
drop index cross_test_table_index;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
select * from cross_test_table order by 1;
drop table     cross_test_table;

create table cross_test_table
(
	c1 int,
	c2 int
)
partition by range (c1)
(
	partition p0_cross_test_table values less than (50),
	partition p1_cross_test_table values less than (100),
	partition p2_cross_test_table values less than (150)
);
create index cross_test_table_index on cross_test_table(c1) local;
alter table cross_test_table drop partition p1_cross_test_table;
insert into cross_test_table values(50, 100),(99,200),(20,200),(30,100),(146,200);
select * from cross_test_table order by 1;
delete from cross_test_table where c1 = 50;
select * from cross_test_table order by 1;
alter table cross_test_table add partition p3_cross_test_table values less than (200);
insert into cross_test_table values(171,100),(199,200);
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
drop index cross_test_table_index;
select a.relname ,p.relname from pg_partition p, pg_partition a, pg_class c where  p.parentid = c.oid  and  c.relname = 'cross_test_table_index' and a.oid = p.indextblid order by 1;
select * from cross_test_table order by 1;
drop table     cross_test_table;

--9 drop partition insert values  delete value create index drop index -------------------------------------- end

