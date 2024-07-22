-- CREATE DATABSE IF NOT EXIST
CREATE DATABASE IF NOT EXISTS test_db;

USE test_db;

-- SHOW ALL DATABASE SES
SHOW DATABASES;

-- SHOW ALL TABLES
SHOW TABLES;


-- CREATE TABLE
CREATE TABLE IF NOT EXISTS fruits(
name string,
price int
);

-- SHOW STRUCTURE
DESCRIBE fruits;

-- INSERT DATA
INSERT INTO fruits(name, price) VALUES('MANGO',25.0);
INSERT INTO fruits(name, price) VALUES('APPLE',10.0);
INSERT INTO fruits(name, price) VALUES('BANANA',5.0);

-- SELECT DATA
SELECT * FROM fruits;

-- TRUNCATE TABLE
TRUNCATE TABLE fruits;

-- DROP TABLE 
DROP TABLE fruits;

-- LOAD DATA INTO TABLE
CREATE TABLE IF NOT EXISTS customers(
index int,
customerid string,
first_name string,
last_name string,
company string,
city string,
country string,
phone string,
email string,
subscription_date date,
website string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

SELECT * FROM customers;

-- LOAD DATA FROM FILE CSV
LOAD DATA LOCAL INPATH '/opt/hive/host_data/data_db/customers.csv' OVERWRITE INTO TABLE customers;

--  ENABLED ACID TRANSACTION TABLE
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
-- The follwoing are not required if you are using Hive 2.0
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nostrict;
-- The following parameters are required for standalone hive metastore
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;


-- DISABLE TRANSACTION
SET hive.support.concurrency=false;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.enforce.bucketing=false;
SET hive.exec.dynamic.partition.mode=strict;
SET hive.compactor.initiator.on=false;
SET hive.compactor.worker.threads=0;

-- CREATE TRANSATIONAL TABLE
CREATE TABLE employee_trans(
id int,
name string,
age int,
gender string
) STORED AS ORC TBLPROPERTIES('transactional'='true');

-- INSERT DATA
INSERT INTO employee_trans VALUES(1,'James',30,'M'),(2,'Ann',40,'F'),(3,'Jeff',41,'M'),(4,'Jennifer',20,'F');

SELECT * FROM employee_trans;

UPDATE employee_trans SET
age=45
WHERE id=3;

-- Other Hive ACID Transactional Commands
SHOW TRANSACTIONS;

-- SHOW locks
SHOW LOCKS

SHOW LOCKS;
SHOW LOCKS <database_name>;
SHOW LOCKS <table_name>;
SHOW LOCKS <table_name> PARTITION (<partition_spec>);

-- Using Joins in Hive
CREATE TABLE customer (
customer_id INT,
customer_name STRING
);

-- INSERT DATA
INSERT INTO customer VALUES(101, 'John'),(102, 'Jane'),(103, 'Bob'),(105, 'Alice');

SELECT * FROM customer;

CREATE TABLE orders (
order_id INT,
customer_id INT,
product STRING
);

-- INSERT DATA
INSERT INTO orders VALUES(1, 101, 'Laptop'),(2, 102, 'Phone'),(3, 103, 'Tablet'),(4, 104, 'Camera'),(5, 104, 'Phone');


SELECT orders.order_id, customer.customer_name, orders.product
FROM orders 
JOIN customer ON orders.customer_id = customer.customer_id;


CREATE TABLE sales (
transaction_id INT,
product_name STRING,
category STRING,
price DECIMAL,
quantity INT,
sale_date DATE
);

INSERT INTO sales VALUES(1, 'Product_A', 'Electronics', 500.00, 2, '2024-03-01'),
(2, 'Product_B', 'Clothing', 35.50, 3, '2024-03-02'),
(3, 'Product_C', 'Books', 20.00, 1, '2024-03-03'),
(4, 'Product_A', 'Electronics', 500.00, 1, '2024-03-04'),
(5, 'Product_B', 'Clothing', 35.50, 2, '2024-03-05');

SELECT * FROM sales;

CREATE TABLE IF NOT EXISTS employee (
id int,
name string,
age int,
gender string,
salary decimal
)
COMMENT 'Employee Table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

-- INSERT DATA
INSERT INTO employee VALUES(1, 'James', 30, 'M', 10000),(2, 'Ann', 40, 'F', 20000),(3, 'Jeff', 41, 'M', 30000),
(4, 'Jennifer', 20, 'F', 40000),(5, 'Sara', 30, 'F', ' '),(7, 'Jeni', ' ', ' ', ' ');

SELECT * FROM employee;

SELECT salary, isnull(salary) FROM employee;

SELECT salary, isnotnull(salary) FROM employee WHERE gender ='M';

SELECT salary, if(isnull(salary),'No Salary','Present Salary')
FROM employee WHERE gender ='F';

SELECT salary,Nvl(salary,-1) FROM employee WHERE gender ='F';


SELECT salary,CASE salary WHEN 10000 THEN 1
WHEN 30000 THEN 2
ELSE 0
END
FROM employee where gender ='F';

SELECT salary,gender,
CASE WHEN salary=10000 THEN 1
WHEN gender='M' THEN 2
ELSE '-1' END
FROM employee WHERE gender = 'M';

