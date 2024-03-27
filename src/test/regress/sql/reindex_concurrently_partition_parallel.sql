--
-- REINDEX CONCURRENTLY PARTITION PARALLEL
--
drop table if exists t2;

CREATE TABLE t2 (id int, data text) partition by range(id)(partition p1 values less than(100), partition p2 values less than(200), partition p3 values less than(MAXVALUE));
insert into t2 select generate_series(1,500),generate_series(1,500);
create index ind_id on t2(id) LOCAL;

select * from t2 where id = 4;

\parallel on
REINDEX index CONCURRENTLY ind_id;
select * from t2 where id = 3;
\parallel off

\parallel on
REINDEX index CONCURRENTLY ind_id;
delete from t2 where id = 4;
\parallel off

\parallel on
REINDEX index CONCURRENTLY ind_id;
insert into t2 values (4,3);
\parallel off

\parallel on
REINDEX index CONCURRENTLY ind_id;
update t2 set data = 4 where id = 4;
\parallel off

\parallel on
REINDEX index CONCURRENTLY ind_id;
select * from t2 where id = 4;
\parallel off

select * from t2 where id = 4;
drop table t2;
