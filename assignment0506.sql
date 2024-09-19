-- CREATE DATABSE IF NOT EXIST
CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

-- SHOW ALL DATABASE SES
SHOW DATABASES;

-- SHOW ALL TABLES
SHOW TABLES;

--  ENABLED ACID TRANSACTION TABLE
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE IF NOT EXISTS customers8982860(
customer_id int,
name string,
age int,
email string
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');


CREATE TABLE IF NOT EXISTS orders8982860(
order_id int,
customer_id int,
order_date string,
amount float
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

