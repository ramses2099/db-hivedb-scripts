# INSTALL HIVE

# PULL IMAGE
docker pull apache/hive:3.1.3

# CREATE THE CONTAINER
docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:3.1.3

# ACCESS TO THE CONTAINER
docker exec -it hive4 bash

docker exec -it [name of container] bash

# ACCCESS TO THE HIVE DATABASE
/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000

# SHOW ALL DATABASE
show databases;

# USE DEFAULT DATABASE
use default;

#CREATE DATABASE IF NO EXISTS 
create database if not exists testdb;
use testdb;


# CREATE TABLE
create table fruits (name string, price int);

# SHOW ALL TABLES
show tables;

# SHOW STRUCTURE 
describe fruits;

# INSERT DATA:
insert into fruits values ('banana', '1.00');
insert into fruits values ('apples', '2.00');

# SELECT DATA
select * from fruits ;

# TRUNCATE TABLE
truncate table fruits

# SELECT DATA
select * from fruits ;

# DELTE DATA 
delete from fruits where name = 'banana';

 # CREATE TABLES CUSTOMERS:
create table customers ( index int , customerid string , first_name string, last_name
string, company string, city string, country string, phone string, email string,
Subscription_Date date, Website string ) ROW FORMAT DELIMITED FIELDS TERMINATED
BY ',';

# LOAD THE DATA FROM FILE:
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/files/customers.csv' OVERWRITE INTO TABLE customers;

#  ENABLED ACID TRANSACTION TABLE
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
# The follwoing are not required if you are using Hive 2.0
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nostrict;
# The following parameters are required for standalone hive metastore
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;


# DISABLE TRANSACTION
SET hive.support.concurrency=false;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
SET hive.enforce.bucketing=false;
SET hive.exec.dynamic.partition.mode=strict;
SET hive.compactor.initiator.on=false;
SET hive.compactor.worker.threads=0;

# CREATE TABLE TRANSACTION
CREATE TABLE my_transactional_table (
id INT,
name STRING
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO my_transactional_table values (1, 'Carlos');
SELECT * FROM my_transactional_table;


CREATE TABLE employee_trans (
id int,
name string,
age int,
gender string)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

INSERT INTO employee_trans VALUES(1,'James',30,'M');
INSERT INTO employee_trans VALUES(2,'Ann',40,'F');
INSERT INTO employee_trans VALUES(3,'Jeff',41,'M');
INSERT INTO employee_trans VALUES(4,'Jennifer',20,'F');

DESCRIBE FORMATTED employee_trans;

UPDATE employee_trans
SET age=45
WHERE id=3;

# Other Hive ACID Transactional Commands
SHOW TRANSACTIONS;

# SHOW locks
SHOW LOCKS

SHOW LOCKS;
SHOW LOCKS <database_name>;
SHOW LOCKS <table_name>;
SHOW LOCKS <table_name> PARTITION (<partition_spec>);

# Using Joins in Hive
CREATE TABLE customer (
customer_id INT,
customer_name STRING
);

INSERT INTO customer VALUES(101, 'John');
INSERT INTO customer VALUES(102, 'Jane');
INSERT INTO customer VALUES(103, 'Bob');
INSERT INTO customer VALUES(105, 'Alice');

CREATE TABLE orders (
order_id INT,
customer_id INT,
product STRING
);

INSERT INTO orders VALUES(1, 101, 'Laptop');
INSERT INTO orders VALUES(2, 102, 'Phone');
INSERT INTO orders VALUES(3, 103, 'Tablet'),(4, 104, 'Camera');
INSERT INTO orders VALUES (5, 104, 'Phone');

# SELECT Joins
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

INSERT INTO sales VALUES(1, 'Product_A', 'Electronics', 500.00, 2, '2024-03-01');
INSERT INTO sales VALUES(2, 'Product_B', 'Clothing', 35.50, 3, '2024-03-02');
INSERT INTO sales VALUES(3, 'Product_C', 'Books', 20.00, 1, '2024-03-03');
INSERT INTO sales VALUES(4, 'Product_A', 'Electronics', 500.00, 1, '2024-03-04');
INSERT INTO sales VALUES(5, 'Product_B', 'Clothing', 35.50, 2, '2024-03-05');


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


INSERT INTO employee VALUES(1, 'James', 30, 'M', 10000);
INSERT INTO employee VALUES(2, 'Ann', 40, 'F', 20000);
INSERT INTO employee VALUES(3, 'Jeff', 41, 'M', 30000);
INSERT INTO employee VALUES(4, 'Jennifer', 20, 'F', 40000);
INSERT INTO employee VALUES(5, 'Sara', 30, 'F', ' ');
INSERT INTO employee VALUES(7, 'Jeni', ' ', ' ', ' ');

SELECT salary, isnull(salary) FROM employee;

SELECT salary, isnotnull(salary) FROM employee WHERE gender ='M';

SELECT salary,if(isnull(salary),'No Salary','Present Salary')
FROM employee WHERE gender='F';


SELECT salary,Nvl(salary,-1) FROM employee WHERE gender='F';

SELECT salary,CASE salary WHEN 10000 THEN 1
WHEN 30000 THEN 2
ELSE 0
END
FROM employee where gender='F';


select salary,gender,
case when salary=10000 then 1
when gender='M' then 2
else '-1' END
from employee where gender = 'M';

# Run latest
export HIVE_VERSION=4.0.0
docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:${HIVE_VERSION}

# Enter to the docker hive
docker exec -it hive4 beeline -u 'jdbc:hive2://localhost:10000/'


# Accessing HiveServer2 Web UI:

Accessed on browser at http://localhost:10002/