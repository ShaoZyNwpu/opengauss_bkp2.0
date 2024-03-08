--Anonymous blocks test
--AnonyAdapt_001: body is null
BEGIN
	    NULL;
END;
/
--AnonyAdapt_002: no var and exception, simple sql include DDL/DCL/DML
BEGIN
	drop table IF EXISTS table5;
	create table table5(i int);
	insert into table5 values (10);
	alter table table5 add j int;
	insert into table5 values (1,2);
END;
/
select * from table5 order by i;
drop table IF EXISTS table5;

--AnonyAdapt_003: including variable(int.varchar2.text)
DECLARE
	r record;
BEGIN
	   FOR r IN 1 .. 3
	    LOOP
	        RAISE NOTICE '%', r;
	   END LOOP;
END;
/

DECLARE
	    my_var VARCHAR2(30);
BEGIN
	    my_var :='world';
END;
/

DECLARE
	my_numeric numeric :=1.2;
	my_smallint smallint :=1;
begin
		RAISE NOTICE '%',my_numeric+my_smallint;
end;
/

DECLARE
	 my_char char;
	 my_varchar varchar;
begin
		my_char :='C';
		my_varchar :='world';
end;
/

DECLARE
	my_raw  raw :='12';
	my_bool boolean :=1;
	my_interval interval :='3 4:05:07';
	my_date date :='2012-08-28';
begin
		RAISE NOTICE '%',my_bool;
		RAISE NOTICE '%',my_interval;
end;
/

--AnonyAdapt_004: including deal with exception in anonymous block
DECLARE
	    a INTEGER := 0;
	    b INTEGER := 1;
	    c INTEGER;
BEGIN
	    c := b / a;
--	EXCEPTION
--	    WHEN OTHERS THEN
END;
/

--nonyAdapt_005: call function in anonymous block
BEGIN
END;
/

DECLARE
	  MYINTEGER INTEGER;
BEGIN
	  MYINTEGER :=1;
	  raise notice 'MYINTEGER is %', MYINTEGER;
END;
/

BEGIN
END;
/

--AnonyAdapt_006: execute dynamic statement in anonymous block
drop table IF EXISTS testto;
create table testto(id integer,name varchar2(50));
INSERT INTO testto VALUES (1,'xxx');

DECLARE
	  MYINTEGER INTEGER ;
	  NAME      VARCHAR2(50);
	  PSV_SQL   VARCHAR2(200);
BEGIN
	     MYINTEGER := 1;
	     PSV_SQL := 'select name from testto where id = $1';
	     EXECUTE PSV_SQL into NAME USING MYINTEGER;
	     raise notice 'NAME is %', NAME;
END;
/

--block with label
begin
			<<l_outer_block>>
		DECLARE
			   visibleValue VARCHAR2(30);
			   hiddenValue VARCHAR2(30);
			BEGIN
			   visibleValue := 'visibleValue';
			   hiddenValue := 'hiddenValue';


			   DECLARE
			      hiddenValue NUMBER(10);
			   BEGIN
			      l_outer_block.hiddenValue := 'inner hiddenValue';
--			   EXCEPTION
--			      WHEN OTHERS
--			      THEN
			   END;
			END;
END;
	/

--AnonyAdapt_009: more begin ..end in anonymous block
DECLARE
	l_param1 int;
	l_param2 int;
	l_param3 int;
begin
	  l_param1 :=1;
	  l_param2 :=2;
	  begin
	  l_param3 :=l_param1+l_param2;
	  end;
	  raise info 'the total is %', l_param3;
end;
/

DECLARE
	l_param1 int;
	l_param2 int;
	l_param3 int;
	l_param4 numeric;
begin
	  l_param1 :=1;
	  l_param2 :=2;
	 begin
	  l_param3 :=l_param1+l_param2;
	  end;
	  begin
	  l_param4:=sqrt(l_param3);
	 end;
	  raise info 'the total is %', l_param3;
	  raise info 'the l_param4 is %',l_param4;
end;
/
--multilayer begin end match
declare
	 PSV_SQL   VARCHAR2(200);
	  NAME      VARCHAR2(50);
	 myinteger int;
begin
	 create table test_table(id int, name varchar2(20));
	 myinteger :=1;
	  begin
	     begin
	        begin
	        insert into test_table values(1,'x');
	        end;
	        insert into test_table values(11,'xx');
	     end;
	        insert into test_table values(111,'xxx');
	   end;
	    PSV_SQL := 'select name from test_table where id = $1';
	    execute PSV_SQL into NAME using myinteger;
	    raise info 'the name is %', NAME;
	    drop table test_table;
end;
/
 --AnonyAdapt_010 --begin end do not match
declare
	 PSV_SQL   VARCHAR2(200);
	  NAME      VARCHAR2(50);
	 myinteger int;
begin
	 create table test_table(id int, name varchar2(20));
	 myinteger :=1;
	  begin
	     begin
	        begin
	        insert into test_table values(1,'x');
	        end;
	        insert into test_table values(11,'xx');
	     end;
	        insert into test_table values(111,'xxx');
	    PSV_SQL := 'select name from test_table where id = $1';
	    execute PSV_SQL into NAME using myinteger;
	    raise info 'the name is %', NAME;
	    drop table test_table;
end;
/

--AnonyAdapt_012: the type of parameter do not match
declare
	begin
	create table my_table( i int);
	insert into my_table values('in');
end;
/
--support '' in dynamic SQL statements when executing anonymous block. l0216466
CREATE OR REPLACE PROCEDURE "sp_hw_sub_addmodules"
(
	v_RETURNCODE	OUT		INTEGER,
	PSV_MODULEDESC	IN		VARCHAR2
)
AS
BEGIN
END;
/

--test execute immediate multi-query with semi end and without semi end.
BEGIN
EXECUTE IMMEDIATE 'start transaction;insert into without_semi_end values(10);end';
END;
/

BEGIN
EXECUTE IMMEDIATE 'start transaction;insert into with_semi_end values(10);end;';
END;
/


--add begin transaction back
create table test_table(id int, name varchar2(20));
insert into test_table values(1,'x');
insert into test_table values(11,'xx');
insert into test_table values(111,'xxx');
create table my_table( i int);
insert into test_table values(1);
begin;
insert into test_table values(1,'abc');
rollback;

begin work;
insert into test_table values(1,'def');
rollback;

begin transaction;
insert into test_table values(1, 'hij');
rollback;

begin;
end;

begin
transaction;
end;

BEGIN
TRANSACTION;
end;

begin

read

write;


declare foo cursor for select * from pg_class;
declare "begin" cursor for select * from pg_class;
declare foo1 cursor for select 'begin', * from pg_class;
declare a int; cursor C1 is select * from pg_class; begin fetch from c1; end;
/
end;


begin;
declare foo cursor with hold for select * from test_table where id > 1;
declare foo1 cursor with hold for select * from test_table, my_table where id != i;
end;

fetch from foo;
fetch from foo1;
close foo;
close foo1;
drop table test_table;
drop table my_table;

--add begin transaction back
create table test_table(id int, name varchar2(20));
insert into test_table values(1,'x');
insert into test_table values(11,'xx');
insert into test_table values(111,'xxx');
create table my_table( i int);
insert into test_table values(1);
begin;
insert into test_table values(1,'abc');
rollback;

begin work;
insert into test_table values(1,'def');
rollback;

begin transaction;
insert into test_table values(1, 'hij');
rollback;

begin
transaction;
end;

BEGIN
TRANSACTION;
end;

begin

read

write;

declare foo cursor for select * from pg_class;
declare "begin" cursor for select * from pg_class;
declare foo1 cursor for select 'begin', * from pg_class;
declare a int; cursor C1 is select * from pg_class; begin fetch from c1; end;
/
end;


begin;
declare foo cursor for select * from test_table where id > 1;
declare foo1 cursor for select * from test_table, my_table where id != i;
fetch from foo;
fetch from foo1;
close foo;
close foo1;
end;
drop table test_table;
drop table my_table;

--when begin and end are in one sql
begin; select 1; end;
begin work; select 1; end;
begin transaction; select 1; end;

begin ; select 1; end;
begin work ; select 1; end;
begin transaction ; select 1; end;
--cases not terminated by ';'
begin
/
end;

begin
/
end;

begin work
/
end;

begin transaction
/
end;
begin work
/
end;
