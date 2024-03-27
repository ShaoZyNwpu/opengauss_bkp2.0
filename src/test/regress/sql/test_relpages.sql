-- analyze db after delete data in DN1.
create table xt_relpages(a int);
insert into xt_relpages values(generate_series(1,99));
delete from xt_relpages where xc_node_id= (select node_id from pgxc_node where node_name='datanode1');
analyze xt_relpages;
select relname, relpages, reltuples from pg_class where relname = 'xt_relpages';

create table xt_relpages_2(a int);
insert into xt_relpages_2 select * from xt_relpages;
analyze xt_relpages_2;
select relname, relpages, reltuples from pg_class where relname = 'xt_relpages_2';

drop table xt_relpages;
drop table xt_relpages_2;
