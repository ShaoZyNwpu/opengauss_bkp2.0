CREATE SCHEMA autovac_test;
SET search_path TO autovac_test;

create table autovac_test.test_part (
col0 int NOT NULL,
col1 int
)

WITH (orientation=column)
PARTITION BY RANGE (col0)
(
 PARTITION p0 VALUES LESS THAN (10000000),
 PARTITION p1 VALUES LESS THAN (20000000),
 PARTITION p2 VALUES LESS THAN (30000000),
 PARTITION p3 VALUES LESS THAN (40000000),
 PARTITION p4 VALUES LESS THAN (50000000),
 PARTITION p5 VALUES LESS THAN (60000000),
 PARTITION p6 VALUES LESS THAN (70000000),
 PARTITION p7 VALUES LESS THAN (80000000),
 PARTITION p8 VALUES LESS THAN (90000000),
 PARTITION p9 VALUES LESS THAN (100000000)
);

insert into autovac_test.test_part values(1234567,123);
insert into autovac_test.test_part values(1234567,123);
insert into autovac_test.test_part values(1234567,123);
insert into autovac_test.test_part values(12345678,23);
insert into autovac_test.test_part values(12345678,23);
insert into autovac_test.test_part values(12345678,23);
insert into autovac_test.test_part values(12345678,123);
insert into autovac_test.test_part values(12345678,123);
insert into autovac_test.test_part values(12345678,123);
insert into autovac_test.test_part values(12345678,123);
insert into autovac_test.test_part values(32345678,123);
insert into autovac_test.test_part values(32345678,123);
insert into autovac_test.test_part values(32345678,123);
insert into autovac_test.test_part values(32345678,123);
insert into autovac_test.test_part values(32345678,23);
insert into autovac_test.test_part values(32345678,23);
insert into autovac_test.test_part values(32345678,23);
insert into autovac_test.test_part values(42345678,23);
insert into autovac_test.test_part values(42345678,23);
insert into autovac_test.test_part values(42345678,23);
insert into autovac_test.test_part values(42345678,123);
insert into autovac_test.test_part values(42345678,123);
insert into autovac_test.test_part values(52345678,123);
insert into autovac_test.test_part values(52345678,123);
insert into autovac_test.test_part values(52345678,123);
insert into autovac_test.test_part values(62345678,123);
insert into autovac_test.test_part values(62345678,123);
insert into autovac_test.test_part values(62345678,12);
insert into autovac_test.test_part values(72345678,12);
insert into autovac_test.test_part values(72345678,12);
insert into autovac_test.test_part values(72345678,12);
insert into autovac_test.test_part values(72345678,12);
insert into autovac_test.test_part values(82345678,123);
insert into autovac_test.test_part values(82345678,123);
insert into autovac_test.test_part values(92345678,123);
insert into autovac_test.test_part values(92345678,123);

select pg_sleep(1);
select pg_stat_get_tuples_changed('autovac_test.test_part'::regclass);

analyze autovac_test.test_part;

select pg_sleep(1);
select pg_stat_get_tuples_changed('autovac_test.test_part'::regclass);

insert into autovac_test.test_part values(82345678,123);                                       
insert into autovac_test.test_part values(82345678,123);
insert into autovac_test.test_part values(82345678,123);                                       
insert into autovac_test.test_part values(82345678,123);
delete from autovac_test.test_part where col1=23;
update autovac_test.test_part set col1=1234 where col1=123;

select pg_sleep(1);
select pg_stat_get_tuples_changed('autovac_test.test_part'::regclass);

start transaction;
insert into autovac_test.test_part values(1234567,123);
insert into autovac_test.test_part values(32345678,123);
insert into autovac_test.test_part values(32345678,23);
insert into autovac_test.test_part values(42345678,123);
insert into autovac_test.test_part values(52345678,123);
insert into autovac_test.test_part values(62345678,123);
insert into autovac_test.test_part values(62345678,12);
insert into autovac_test.test_part values(72345678,12);
insert into autovac_test.test_part values(82345678,123);
insert into autovac_test.test_part values(92345678,123);

select pg_catalog.pg_stat_get_xact_partition_tuples_inserted(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p3';

update autovac_test.test_part set col1 = col1+1;
select pg_catalog.pg_stat_get_xact_partition_tuples_updated(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p6';
select pg_catalog.pg_stat_get_xact_partition_tuples_hot_updated(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p2';

delete from autovac_test.test_part;
select pg_catalog.pg_stat_get_xact_partition_tuples_deleted(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p1';
commit;

select pg_catalog.pg_autovac_timeout('autovac_test.test_part'::regclass);  
select pg_catalog.pg_autovac_coordinator('autovac_test.test_part'::regclass);
select pg_stat_get_tuples_changed('autovac_test.test_part'::regclass); 
select pg_catalog.pg_stat_get_partition_tuples_changed(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p9';
select pg_catalog.pg_stat_get_partition_dead_tuples(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p8';
select pg_catalog.pg_stat_get_partition_tuples_inserted(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p3';
select pg_catalog.pg_stat_get_partition_tuples_updated(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p6';
select pg_catalog.pg_stat_get_partition_tuples_deleted(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p1';
select pg_catalog.pg_stat_get_partition_tuples_hot_updated(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p2';
select pg_catalog.pg_stat_get_partition_live_tuples(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p0';
select pg_catalog.pg_stat_get_partition_dead_tuples(p.oid) from pg_class c,pg_partition p where p.parentid=c.oid and c.oid='autovac_test.test_part'::regclass and p.relname='p0';
select pg_stat_get_tuples_changed('autovac_test.test_part'::regclass);

select pg_sleep(1);

select pg_stat_get_db_tuples_deleted(oid) from pg_database where datname='postgres';
select pg_stat_get_db_tuples_inserted(oid) from pg_database where datname='postgres';
select pg_stat_get_db_tuples_updated(oid) from pg_database where datname='postgres';


select count(1) from pg_catalog.pg_total_autovac_tuples(true);
select * from pg_autovac_status('autovac_test.test_part'::regclass);

RESET search_path;
DROP SCHEMA autovac_test CASCADE;
