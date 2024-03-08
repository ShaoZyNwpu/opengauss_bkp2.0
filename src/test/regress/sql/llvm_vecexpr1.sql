/* 
 * This file is used to test the function of vecexpression.cpp with LLVM Optimization
 * It is only an auxiliary file to vexecpression.sql to cover the basic functionality 
 * about expression.
 */
/********************************
    T_Var,
    T_Const,
    T_Case,
    T_OpExpr,
    T_ArrayExpr,
    T_BoolenTest,
    T_NullTest,
    T_NULLIF,
    T_BOOL (AND/OR/NOT)
********************************/
----
--- Create Table and Insert Data
----
drop schema if exists llvm_vecexpr_engine1 cascade ;
create schema llvm_vecexpr_engine1;
set current_schema = llvm_vecexpr_engine1;
set codegen_cost_threshold=0;

CREATE TABLE llvm_vecexpr_engine1.LLVM_VECEXPR_TABLE_01(
    a      int,
    b      int,
    c      int 
) WITH (orientation=column);

COPY LLVM_VECEXPR_TABLE_01(a, b, c) FROM stdin;
1	1	1
1	1	2
1	\N	3
1	1	4
1	2	1
1	2	2
1	3	3
2	1	1
2	1	2
2	1	3
2	1	4
\N	2	1
2	2	2
2	2	3
2	2	4
2	\N	3
2	3	2
2	3	3
\N	3	\N
3	1	1
3	1	\N
3	1	3
3	1	4
3	2	3
3	2	1
4	0	0
\N	\N	\N
4	\N	3
2	2	\N
\N	1	3
\.

CREATE TABLE llvm_vecexpr_engine1.LLVM_VECEXPR_TABLE_02(
    col_int	int,
    col_bigint	bigint,
    col_float	float4,
    col_float8	float8,
    col_char	char(10),
    col_bpchar	bpchar,
    col_varchar	varchar,
    col_text1	text,
    col_text2   text,
    col_num1	numeric(10,2),
    col_num2	numeric,
    col_date	date,
    col_time    time
)with(orientation=column);

COPY LLVM_VECEXPR_TABLE_02(col_int, col_bigint, col_float, col_float8, col_char, col_bpchar, col_varchar, col_text1, col_text2, col_num1, col_num2, col_date, col_time) FROM stdin;
1	256	3.1	3.25	beijing	AaaA	newcode	myword	myword1	3.25	3.6547	2011-11-01 00:00:00	2017-09-09 19:45:37
3	12400	2.6	3.64755	hebei	BaaB	knife	sea	car	1.62	3.64	2017-10-09 19:45:37	2017-10-09 21:45:37
5	25685	1.0	25	anhui	CccC	computer	game	game2	7	3.65	2012-11-02 00:00:00	2018-04-09 19:45:37
-16	1345971420	3.2	2.15	hubei	AaaA	phone	pen	computer	4.24	6.36	2012-11-04 00:00:00	2012-11-02 00:03:10
64	-2566	1.25	2.7	jilin	DddD	girl	flower	window	65	69.36	2012-11-03 00:00:00	2011-12-09 19:45:37
\N	256	3.1	4.25	anhui	BbbB	knife	phone	light	78.12	2.35684156	2017-10-09 19:45:37	1984-2-6 01:00:30
81	\N	4.8	3.65	luxi	EeeE	girl	sea	crow	145	56	2018-01-09 19:45:37	2018-01-09 19:45:37
25	365487	\N	3.12	lufei	EeeE	call	you	them	7.12	6.36848	2018-05-09 19:45:37	2018-05-09 19:45:37
36	5879	10.15	\N	lefei	GggG	call	you	them	2.5	2.5648	2015-02-26 02:15:01	1984-2-6 02:15:01
27	256	4.25	63.27	\N	FffF	code	throw	away	2.1	25.65	2018-03-09 19:45:37	2017-09-09 14:20:00
9	-128	-2.4	56.123	jiangsu	\N	greate	book	boy	7	-1.23	2017-12-09 19:45:37	 2012-11-02 14:20:25
1001	78956	1.25	2.568	hangzhou	CccC	\N	away	they	6.36	58.254	2017-10-09 19:45:37	1984-2-6 01:00:30
2005	12400	12.24	2.7	hangzhou	AaaA	flower	\N	car	12546	3.2546	2017-09-09 19:45:37	2012-11-02 00:03:10
8	5879	\N	1.36	luxi	DeeD	walet	wall	\N	2.58	3.54789	2000-01-01	2000-01-01 01:01:01
652	25489	8.88	1.365	hebei	god	piece	sugar	pow	\N	2.1	2012-11-02 00:00:00	2012-11-02 00:00:00
417	2	9.19	0.256	jiangxi	xizang	walet	bottle	water	11.50	-1.01256	\N	1984-2-6 01:00:30
18	65	-0.125	78.96	henan	PooP	line	black	redline	24	3.1415926	2000-01-01	\N
\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N	\N
700	58964785	3.25	1.458	\N	qingdao	\N	2897	dog	9.36	\N	\N	2017-10-09 20:45:37
505	1	3.24	\N	\N	BbbB	\N	myword	pen	147	875	2000-01-01 01:01:01	2000-01-01 01:01:01
3	12400	2.6	3.64755	 	  	   	 	  	1.62	3.64	2017-10-09 19:45:37	2017-10-09 21:45:37
\.

create table llvm_vecexpr_engine1.LLVM_VECEXPR_TABLE_03
(
    cjrq	timestamp(0) without time zone,
    cjxh	character(10),
    zqdh	character(10),
    bxwdh	character(6),
    bgddm	character(10),
    bhtxh	character(10),
    brzrq	character(1),
    bpcbz	character(1),
    sxwdh	character(6),
    sgddm	character(10),
    shtxh	character(10),
    srzrq	character(1),
    spcbz	character(1),
    cjgs	numeric(9,0),
    cjjg	numeric(9,3),
    cjsj	numeric(8,0),
    ywlb	character(1),
    mmlb	character(1),
    ebbz	character(1),
    filler	character(1)
)WITH (orientation=column, compression=no);


insert into LLVM_VECEXPR_TABLE_03 (zqdh,cjrq,cjsj,cjgs) values ('233574','2015-05-02',9150000,1);
insert into LLVM_VECEXPR_TABLE_03 (zqdh,cjrq,cjsj,cjgs) values ('233574','2015-05-03',9150001,1);
insert into LLVM_VECEXPR_TABLE_03 (zqdh,cjrq,cjsj,cjgs) values ('233574','2015-05-04',9150002,1);
insert into LLVM_VECEXPR_TABLE_03 (zqdh,cjrq,cjsj,cjgs) values ('233574','2015-05-05',9150003,1);
insert into LLVM_VECEXPR_TABLE_03 (zqdh,cjrq,cjsj,cjgs) values ('233574','2015-05-06',9150004,1);
insert into LLVM_VECEXPR_TABLE_03 (zqdh,cjrq,cjsj,cjgs) values ('233574','2015-05-07',9150005,1);

analyze llvm_vecexpr_table_01;
analyze llvm_vecexpr_table_02;
analyze llvm_vecexpr_table_03;

----
--- case 1 : arithmetic operation
----
explain (verbose on, costs off) select * from llvm_vecexpr_table_01 where a = 1 and b = 3;

select * from llvm_vecexpr_table_01 where a = 1 and b = 3 order by 1, 2, 3;
select * from llvm_vecexpr_table_01 where a > 1 and a < 4 order by 1, 2, 3;
select * from llvm_vecexpr_table_01 where ((a + 1) * 2) -1 > 3 order by 1, 2, 3;
select * from llvm_vecexpr_table_01 where a + b - c > 2 order by 1, 2, 3;

----
--- case 2 : basic comparison operation
----
explain (verbose on, costs off)
select * from llvm_vecexpr_table_02 where col_char = 'beijing' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_char = 'beijing' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where 'beijing' = col_char order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text1 = 'phone' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_time = '19:45:37' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where substr(col_char, 1, 2) = 'lu' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where substr(col_text1, 1, 3) = 'you' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where substr(col_text1, -2, 3) = 'me' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where substr(col_text1, NULL, 2) = 'le' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where btrim(col_char) = 'lufei' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where btrim(col_char) != 'lufei' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_char != 'lufei' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_char != 'hangzhou  ' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where 'hebei     ' != col_char order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where lpad(col_char,9,'ai') = 'aibeijing' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_bigint > -1 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_bigint >= 25689745 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_bigint <= 25689745 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_bigint != 25689745 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_bigint = 128 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_time + '06:00:00' > '01:01:01' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_date + '1000 days 10:10:10' > '2016-12-30 00:00:00' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where substr(col_text1, 5, 2) is not null order by 1, 2, 3, 4, 5, 6, 7;
select * from llvm_vecexpr_table_02 where substr(col_text1, 5, 0) is not null order by 1, 2, 3, 4, 5, 6, 7;
select * from llvm_vecexpr_table_02 where col_char = col_char order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where btrim(col_char) is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where btrim(col_varchar) is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where rtrim(col_bpchar) is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where rtrim(col_text1) is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where to_numeric(col_float8) = col_num1 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where to_number(col_float8) = col_num1 order by 1, 2, 3, 4, 5;

---non-strict function
select * from llvm_vecexpr_table_02 where (col_num1 || col_text1) is not null order by 1, 2, 3, 4, 5;

---null test with different input types
select * from llvm_vecexpr_table_02 where col_float is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_float is not null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_float8 is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_float8 is not null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_num1 is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_num2 is not null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_bpchar is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_char is not null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_varchar is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_varchar is not null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text1 is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text1 is not null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_time is null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_time is not null order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where (col_char||col_text1) is not null order by 1, 2, 3, 4, 5;

----
--- case 3 : Case Expr - int4eq, int8eq, float4eq, float8eq, texteq, bpchareq, 
---          dateeq, time, timetzeq, timestampeq, timestamptzeq
----
explain (verbose on, costs off) select * from llvm_vecexpr_table_01 where 
						case 
						when a = 1 then 11
						when a = 2 then 22
						when a = 3 then 33
						else 44
						end  = 22 order by 1, 2, 3;
 
select * from llvm_vecexpr_table_01 where case when a = 1 then 11
						when a = 2 then 22
						when a = 3 then 33
						else 44
						end  = 22 order by 1, 2, 3;
 												
select * from llvm_vecexpr_table_01 where case a when 1 then 11
						when 2 then 22
						when 3 then 33
						else 44
						end  = 22 order by 1, 2, 3;
 
select * from llvm_vecexpr_table_01 where case a when 1 then 11
						when 2 then 22
						when 3 then 33
						else 44
						end  = 44 order by 1, 2, 3;
 
select * from llvm_vecexpr_table_01 where case when a = 1 then 
								case when b = 1 then 11
								when b = 2 then 12
								else 13
								end
					 	when a = 2 then
								case when b = 1 then 21
									when b = 2 then 22
								else 23
								end
					else 33 end = 12 order by 1, 2, 3;  
 
select * from llvm_vecexpr_table_01 where case a when 1 then 
								case b when 1 then 11
									when 2 then 12
									else 13
									end
					 	when 2 then
								case b when 1 then 21
									when 2 then 22
									else 23
									end
					else 33 end = 12 order by 1, 2, 3;
 			
select * from llvm_vecexpr_table_02 where case when col_float < -10 then 1.0
                                                when col_float < -3 then 0.0
                                                when col_float < 3 then 2.0
                                                when col_float < 10 then 0.0
                                                else 1.0 end < 0.5 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where case when col_bpchar = 'AaaA' then 2 
												when col_bpchar = 'DddD' then 4
												else 6 end < 6 order by 1, 2, 3, 4, 5;
 
select * from llvm_vecexpr_table_02 where case when col_bpchar = 'BbbB' then 2 
												when col_bpchar = 'GggG' then 4
												else 6 end < 8 order by 1, 2, 3, 4, 5;
 
select * from llvm_vecexpr_table_02 where case when  col_char = 'beijing' then 'A'
												when col_char = 'anhui' then 'B'
												when col_char = 'hangzhou' then 'C'
												else 'D' end = 'C' order by 1, 2, 3, 4, 5;
 												
select * from llvm_vecexpr_table_02 where case col_text1 when 'phone' then 'E'
														when 'you' then 'F'
														else 'H' end = 'H' order by 1, 2, 3, 4, 5;
 
select * from llvm_vecexpr_table_02 where case when col_char = 'beijing' then
												case when col_text2 = 'pen' then 'AA'
													when col_text2 = 'them' then 'AB'
													else 'AC' end
												when col_char = 'hebei' then
												case when col_text2 = 'game' then 'BB'
													when col_text2 = 'away' then 'BC'
													else 'BD' end
												else 'CC' end = 'BB' order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where case col_char when 'beijing' then 'DD' when 'anhui' then 'EE' else 'FF' end  = 'DD' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02	where case col_varchar when 'call' then 'DD' when 'girl' then 'EE' else 'FF' end  = 'DD' order by 1, 2, 3, 4, 5;  									
select * from llvm_vecexpr_table_02 where case col_varchar when 'call' then 'DD' when 'girl' then 'EE' else NULL end  = 'DD' order by 1, 2, 3, 4, 5;
 												
select * from llvm_vecexpr_table_02 where case col_varchar when 'call' then 
													case col_text2 when 'car' then 'DD'
																when 'water' then 'DF'
																else 'DH' end
													when 'girl' then 
													case col_text2 when 'crow' then 'EE'
																when 'light' then 'EL'
																else 'EW' end
													else 'TT' end = 'EE' order by 1, 2, 3, 4, 5;
 
select * from llvm_vecexpr_table_02 where case when col_int < 10 then 'A' when col_int > 500 then 'C' else 'B' end = 'B' order by 1, 2, 3, 4, 5;

 
select * from llvm_vecexpr_table_02 where case when substr(col_char, 1, 2) = 'lu' then 'A' 
												when substr(col_char, 1, 2) = 'an' then 'B'
												else 'C' end = 'B' order by 1, 2, 3, 4, 5;
												
 
select * from llvm_vecexpr_table_02 where case substr(col_text1, 1, 3) when 'you' then 'A' when 'sea' then 'B' else 'C' end = 'A' order by 1, 2, 3, 4, 5;


select * from llvm_vecexpr_table_02 where case when substr(col_char, 1, 2) = 'lu' then
													case when substr(col_text1, 1, 3) = 'you' then 'AA'
															when substr(col_text1, 1, 3) = 'sea' then 'AB'
															else 'AC' end
												when substr(col_char, 1, 2) = 'an' then
													case when substr(col_text1, 1, 3) = 'you' then 'BB'
															when substr(col_text1, 1, 3) = 'sea' then 'BC'
															else 'BD' end
												else 'AA' end = 'AA' order by 1, 2, 3, 4, 5;
												

select * from llvm_vecexpr_table_02 where case substr(col_char, 1, 2) when 'lu' then
													case substr(col_text1, 1, 3) when 'you' then 'AA'
															when 'sea' then 'AB'
															else 'AC' end
												when 'an' then
													case substr(col_text1, 1, 3) when 'you' then 'BB'
															when 'sea' then 'BC'
															else 'BD' end
												else 'AA' end = 'BC' order by 1, 2, 3, 4, 5;
					
select * from llvm_vecexpr_table_02 where case when col_time < '6:00:00'::time then 0.0
                                                when col_time < '12:00:00'::time then 1.0
                                                when col_time < '20:00:00'::time then 1.0
                                                else 0.0 end > 0.5 order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where case col_date when '2000-01-01 01:01:01' then 'A' 
														when '2018-05-09 19:45:37' then 'B'
														else 'C' end = 'B' order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where case col_float8 when 2.15::float8 then 2.4::float8 
								when 2.7::float8 then 3.6::float8 
								else 4.8::float8 end = 2.4 order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where case col_num1 when 3.25 then 1.2 when 7.00 then 2.2 when 9.36 then 3.33 end > 1.2  order by 1, 2, 3;

----
--- case 4 : ArrayOp Expr - int4eq, int8eq, float4eq, float8eq, bpchareq, texteq, dateeq, substr with texteq, timetz
----
explain (verbose on, costs off) select * from llvm_vecexpr_table_02 where col_int in (2,3,8,16);
select * from llvm_vecexpr_table_02 where col_int in (2,3,8,16) order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where col_int in (NULL, 2, 4, 6, 4, 1001) order by 1, 2, 3, 4, 5;

select col_int, col_bigint, col_float, col_date from llvm_vecexpr_table_02 where col_bigint in (256, NULL, 5879, 365487) order by 1, 2, 3, 4;

select col_int, col_bigint, col_float, col_char, col_num1, col_date from llvm_vecexpr_table_02 where col_char in ('luxi', 'lufei', NULL, 'lufei') order by 1, 2, 3, 4, 5, 6;

select * from llvm_vecexpr_table_02 where col_text1 in ('phone', 'you', 'call', 'sea') order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where col_text1 in ('phone', 'you', NULL) order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where substr(col_char, 1, 3) in ('bei', 'han', 'hen', NULL) order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where substr(col_text1, 1, 3) in ('sea', 'you', 'ccc', 'boy', 'you', NULL) order by 1, 2, 3, 4, 5;

select col_int, col_bigint, col_float, col_char, col_date from llvm_vecexpr_table_02 where col_date in ('2011-11-01', '2000-01-01', '2017-10-09', NULL, '2012-11-03') order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where col_int = any(array[-16, 1, 79]) order by 1, 2, 3, 4, 5;
 
select * from llvm_vecexpr_table_02 where col_bigint = any(array[-128, 256, 5879]) order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where col_float in( 1.0, 1.25, 2.25, 3.25, 4.25, 5.25 ) order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where col_text1 = any(array['window','pen']) order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where col_char = any(array['hangzhou', 'jiangsu', NULL]) order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where col_bpchar = any(array['AaaA', 'BbbB', 'DddD']) order by 1, 2, 3, 4, 5;

select col_float8, col_num1 from llvm_vecexpr_table_02 where col_float8 in (NULL, 3.25, 2.15, 2.7, 3.25, NULL) and col_num1 = any(array[3.25, 7.00, NULL, 65]) order by 1, 2;

----
--- case 5 : Nullif
----
select * from llvm_vecexpr_table_01 where nullif(a, b) is NULL order by 1, 2, 3;

select * from llvm_vecexpr_table_02 where nullif(col_int, 1) is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_int, 1::int2) is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_bigint, 256) is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_bigint, 128::bigint) is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_float8, 3.25) is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_num1, col_num2) is NULL order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_bpchar, 'AaaA') is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_text1, 'myword') is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_date, '2011-11-01 00:00:00') is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_time, '19:45:37') is NULL order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_int, col_bigint) is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_char, 'beijing') is NULL order by 1,2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_varchar, 'flower') is NULL order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(substring(col_text1, 10, 2), 'AB') is NULL order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(substr(col_text1, 10, 2), 'AB') is NULL order by 1, 2, 3, 4, 5;

select * from llvm_vecexpr_table_02 where nullif(col_num1, 2.45) is NULL order by 1, 2, 3, 4, 5;

----
--- case 6 : text like/text not like
----
select * from llvm_vecexpr_table_02 where col_text1 like '%e' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text1 like 'b%' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text1 like '%ow' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_varchar like '%e' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text1 like '%ow_%' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text1 not like '%m' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text2 not like '%o%' order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where col_text2 not like 't%' order by 1, 2, 3, 4, 5;

----
--- case 7 : other cases
----
select * from llvm_vecexpr_table_02 where length(col_text1 || col_text2) + 2 > 5 order by 1, 2, 3, 4, 5;
select * from llvm_vecexpr_table_02 where (col_text1||' ') is not NULL order by 1, 2, 3, 4, 5;

----
--- case 8 : multi IR case
----
set enable_nestloop = off;
set enable_mergejoin = off;
select * from llvm_vecexpr_table_02 A join llvm_vecexpr_table_02 B on A.col_char = B.col_char and A.col_int = B.col_int where A.col_char = 'anhui' and A.col_int < 10 order by 1, 2, 3, 4, 5;

select A.col_int, A.col_bigint, A.col_float from llvm_vecexpr_table_02 A join llvm_vecexpr_table_02 B on A.col_num1 = B.col_num1 where A.col_text1 > B.col_text2 order by 1, 2, 3;

----
---  case 9: codegen_cost_threshold
----
set codegen_cost_threshold = 10000;
explain (verbose on, costs off, analyze on)
select * from llvm_vecexpr_table_02 where col_char = 'beijing' order by 1, 2, 3, 4, 5;

----
----
select zqdh, sum(cjgs), count(*) from llvm_vecexpr_table_03 where zqdh ='233574' and cjrq>='2015-05-02' and cjrq<='2015-06-12' and cjsj>= 9150000 and cjsj<=15300000 group by zqdh;

----
--- clean table and resource
----
drop schema llvm_vecexpr_engine1 cascade;


