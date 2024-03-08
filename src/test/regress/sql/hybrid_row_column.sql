CREATE SCHEMA hybrid_row_column;
SET CURRENT_SCHEMA=hybrid_row_column;

-- Create Table and Insert Data
CREATE TABLE columnar_table_01(id INTEGER,info TEXT) WITH(ORIENTATION=COLUMN);
INSERT INTO columnar_table_01 VALUES(GENERATE_SERIES(1,10),'A');
CREATE TABLE columnar_table_02(id INTEGER,info TEXT);
CREATE TABLE columnar_table_04(id INTEGER,info TEXT) WITH(ORIENTATION=COLUMN);

-- insert into row table from column table with limit
INSERT INTO columnar_table_02 SELECT * FROM columnar_table_01 LIMIT 2;

-- insert into column table from column table with limit
INSERT INTO columnar_table_04 SELECT * FROM columnar_table_01 LIMIT 2;

-- create table as select with limit
CREATE TABLE columnar_table_03 AS SELECT * FROM columnar_table_01 LIMIT 2;
CREATE TABLE columnar_table_05 WITH(ORIENTATION=COLUMN) AS SELECT * FROM columnar_table_01 LIMIT 2;

-- test serialize and deserialize of MACADDR
CREATE TABLE columnar_table_06(c_id INT, c_w_id INT, c_first VARCHAR(100)) WITH (ORIENTATION=COLUMN);
INSERT INTO columnar_table_06 VALUES(3,1,'08-00-2a-01-02-01');
INSERT INTO columnar_table_06 VALUES(3,1,'08-00-2a-01-02-02');
CREATE TABLE row_table_macaddr(id INT, c_macaddr MACADDR);

SELECT c_w_id, '08-00-2a-01-02-03'::MACADDR FROM columnar_table_06;
SELECT c_first::MACADDR FROM columnar_table_06 ORDER BY c_first::MACADDR;
SELECT TRUNC(c_first::MACADDR) FROM columnar_table_06;
SELECT COUNT(c_first) FROM columnar_table_06 GROUP BY c_first::MACADDR;
SELECT * FROM columnar_table_06 ORDER BY c_first::MACADDR;

INSERT INTO row_table_macaddr SELECT c_w_id, '08-00-2a-01-02-03' FROM columnar_table_06;
SELECT * FROM row_table_macaddr;

DROP SCHEMA hybrid_row_column CASCADE;