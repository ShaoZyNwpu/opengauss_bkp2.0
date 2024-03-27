set query_dop=4;
drop table if exists bmsql_item;
CREATE TABLE bmsql_item (
i_id int NoT NULL,
i_name varchar(24),
i_price numeric(5,2),
i_data varchar( 50),
i_im_id int
);
insert into bmsql_item values ('1','sqltest_varchar_1','0.01','sqltest_varchar_1','1') ;
insert into bmsql_item values ('2','sqltest_varchar_2','0.02','sqltest_varchar_2','2') ;
insert into bmsql_item values ('3','sqltest_varchar_3','0.03','sqltest_varchar_3','3') ;
insert into bmsql_item values ('4','sqltest_varchar_4','0.04','sqltest_varchar_4','4') ;
insert into bmsql_item(i_id) values ('5');

select
row_number() over(order by 2)
from (select i_price as c1,'a' as c2 from bmsql_item order by 1 desc) tb1
group by tb1.c2,cube(tb1.c1,tb1.c1)
window window1 as (order by 3)
order by 1;

drop table bmsql_item;