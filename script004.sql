*********************************************************
*********************************************************
                 Hive - Hive Introduction
*********************************************************
*********************************************************
-- Download dataset

wget https://www.dropbox.com/s/ia779cdcjfctd84/stocks


-- Create a directory called hive under your BigData folder
*** REMEMBER this is the folder in HDFS NOT in linux ***

[~]$ hadoop fs -mkdir /BigData

-- Put the stocks dataset into the new directory
-- Here I am assuming the stocks dataset is in your home directory


[~]$ hadoop fs -copyFromLocal /~/stocks /BigData/.

-- Start the hive shell by typing hive in the terminal and pressing enter


[~]$ hive
clea

connect to hive
beeline -u jdbc:hive2://localhost:10000



hive> CREATE DATABASE stocks_db;

hive> USE stocks_db;

hive> CREATE EXTERNAL TABLE IF NOT EXISTS stocks (
ymd STRING,
symbol STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
price_adj_close FLOAT,
volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/BigData/hive/'
TBLPROPERTIES('skip.header.line.count'='1');


hive> SELECT * FROM stocks 
LIMIT 100;


*********************************************************
*********************************************************
               Hive - Hive table in details
*********************************************************
*********************************************************

hive> CREATE DATABASE stocks_db;

hive> SHOW DATABASES;

hive> USE stocks_db;

hive> SHOW TABLES;

hive> DROP DATABASE stocks_db;

hive> DROP TABLE stocks;

hive> DROP DATABASE stocks_db CASCADE;


-- Creating a table skeleton, there is no data in this table yet

hive> CREATE TABLE IF NOT EXISTS stocks (
ymd STRING,
symbol string,
price_open float,
price_high float,
price_low float,
price_close float,
price_adj_close FLOAT,
volume int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES('skip.header.line.count'='1');

-- Basic information about a table
hive > DESCRIBE stocks;

-- detailed information about stocks table can be found with this command
hive> DESCRIBE FORMATTED stocks;



*********************************************************
*********************************************************
		Hive - Loading Hive Tables
*********************************************************
*********************************************************

### CREATE A TABLE FOR STOCKS ###
/*First switch to the database*/

hive> DROP DATABASE stocks_db CASCADE;


hive> CREATE DATABASE stocks_db;

hive> USE stocks_db;

hive> CREATE TABLE IF NOT EXISTS stocks (
ymd STRING,
symbol STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
price_adj_close FLOAT,
volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES (
'creator'='Big Data class', 
'created_on' = '2022-09-27', 
'description'='This table holds daily stocks data.',
'skip.header.line.count'='1');

/*To make sure table is there, use following command */

hive> SHOW tables;


### DESCRIBE TABLE TO GET DETAILS ABOUT TABLE ###

hive> DESCRIBE FORMATTED stocks;

-- The important attribute is "location" - this is the location in HDFS where the data for this table will be stored.

### COPY THE STOCKS DATASET TO HDFS IF YOU HAVEN'T DONE SO###

hadoop fs -copyFromLocal stocks.txt /BigData/hive/stocks

-- check to see if the file is in the directory

hadoop fs -ls /BigData/

*** You can also check HDFS files in Hive shell, just use a exclamation mark (!) to start the command

hive> !hadoop fs -ls /BigData/hive/stocks

### *** METHOD 1 *** LOAD DATASAET USING LOAD COMMAND ###

-- this command MOVES the data from the hive folder into the location folder specified in the Metadata

hive> LOAD DATA INPATH '/BigData/stocks' INTO TABLE stocks;

bigdata/hive/stocks_data.csv

LOAD DATA INPATH '/opt/hive/bigdata/hive/stocks_data.csv' INTO TABLE stocks;

-- Since the file has been be moved, the following directory will be empty

hive> !hadoop fs -ls /BigData/hive/;

--check out the location of the data for this table

hive> DESCRIBE FORMATTED stocks;

-- display the contents of the folder (should see the stocks data)

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks;

-- very powerful, we now have our data in table format and can use familiar SQL queries

hive> SELECT * FROM stocks LIMIT 100;

### *** METHOD 2 *** LOAD DATASET USING ANOTHER TABLE ###

-- This runs a MapReduce job

hive> CREATE TABLE stocks_ctas AS 
		SELECT * FROM stocks;

-- Let's see the location of the data for this table

hive> DESCRIBE FORMATTED stocks_ctas;

-- Look at the location

hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ctas;

### *** METHOD 3a *** LOAD DATASET USING INSERT..SELECT STATEMENT###

-- Assume you already have the table and want to add more data into the table
-- Here is the way to APPEND data to a table (that already exists).

hive> INSERT INTO TABLE stocks_tech
     	SELECT * FROM stocks
		WHERE symbol = 'NVDA'


hive> INSERT INTO TABLE stocks_ctas
SELECT * FROM stocks;


hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ctas;

hive> SELECT * FROM stocks_ctas;


-- multiple inserts

hive> 	FROM stocks 
	INSERT INTO TABLE stocks_ctas SELECT *
	INSERT INTO TABLE table2 SELECT *
	INSERT INTO TABLE table3 SELECT *

### *** METHOD 3b *** LOAD DATASET USING INSERT OVERWRITE ###

-- This command will overwrite the data in an already created table
-- Let's say we don't want TSLA anymore in stocks_tech, we messed it up
-- Rewwrite the table

hive> INSERT OVERWRITE TABLE stocks_tech
    	SELECT * FROM stocks
		WHERE symbol IN ('AAPL', 'MSFT', 'NVDA');

-- You can check and see that the file has been overwritten
hive> !hadoop fs -ls /user/hive/warehouse/stocks_db.db/stocks_ctas;




### LOCATION ATTRIBUTE & LOADING DATA ### 

** Make sure stocks dataset is saved in this hive folder **

hadoop fs -copyFromLocal stocks.txt /BigData/stocks

-- OR do it from HDFS 

hadoop fs -cp /BigData/stocks /BigData/.

-- make sure the copy was successful
 
hadoop fs -ls /BigData/hive/

-- Here we are creating a table and asking hive to use the mentioned location not the default location under warehouse

hive> CREATE TABLE IF NOT EXISTS stocks_loc (
ymd STRING,
symbol STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
price_adj_close FLOAT,
volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/BigData/hive/'
TBLPROPERTIES (
'creator'='Big Data class', 
'created_on' = '2022-09-27', 
'description'='This table holds daily stocks data.',
'skip.header.line.count'='1');

-- you can manually load the data set into this location or use a tool (like a Pig script) to do this overnight

-- If you run a describe formatted command, you'll see that the location is no longer the default warehouse location

hive> DESCRIBE FORMATTED stocks_loc;

-- Now select some rows, you'll see that it will load data

hive> SELECT * FROM stocks_loc LIMIT 100;