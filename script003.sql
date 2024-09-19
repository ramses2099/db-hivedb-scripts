USE test_db;

--  ENABLED ACID TRANSACTION TABLE
SET hive.support.concurrency=true;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


CREATE TABLE IF NOT EXISTS students8982860(
id int,
name string,
age int,
grade string
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');


insert into students8982860 (id, name, age, grade) values (1, 'Mort Teck', 23, 'A'),
(2, 'Caddric Kupisz', 24, 'B'), (3, 'Rakel Ormston', 20, 'C'),
(4, 'Hertha O'' Lone', 20, 'C'), (5, 'Raphaela Waiting', 24, 'A'),
(6, 'Janot Betterton', 25, 'C'),(7, 'Rickard Cubley', 24, 'A'),
(8, 'Jayson Thebes', 23, 'C'),(9, 'Bianka Manoelli', 22, 'C'),
(10, 'Laurette Carverhill', 22, 'C'),(11, 'Ahmad Stapele', 23, 'B'),
(12, 'Nisse Peret', 25, 'B'),(13, 'Willa Norkett', 20, 'A');

select * from students8982860;

select * from students8982860 where age > 22;

insert into students8982860 (id, name, age, grade) values (14, 'Nina', 23, 'B');
select * from students8982860 where id = 14

update students8982860 set
grade  = 'A'
where id = 5

select * from students8982860 where id = 5;

select * from students8982860 where id = 3;
delete from students8982860 where id = 3;

select sum(age) as total_age from students8982860;
select avg(age) as avg_age from students8982860;

-- 10. Write a SQL query to display student names along with a flag indicating whether their
-- age is above the average age using a conditional function.

-- avg 22.923076923076923
select name, CASE age WHEN 22 THEN 'age is equal to avg' ELSE 'age is above the average' END as flag from students8982860;


select * from students8982860 where age between 20 and 24;

select * from students8982860;

update students8982860 SET 
age = age + 1

select * from students8982860 where grade ='A'

-- DROP TABLE courses8982860;

CREATE TABLE IF NOT EXISTS courses8982860(
coursed int,
course_name string,
instructor_name string
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into courses8982860(coursed,course_name,instructor_name) values(101,'Math','Prof. Predor'),(102,'Science','Prof. Jhon Doe'),
(103,'History','Prof. Jhon Doe'), (104,'Physics','Prof. Jhon Doe'), (105,'Chemistry','Prof. Jhon Doe'), (106,'Biology','Prof. Jhon Doe'), 
(107,'Computer Science','Prof. Jhon Doe'),
(108,'Economics','Prof. Jhon Doe'), (109,'Literature','Prof. Jhon Doe'), (110,'Psychology','Prof. Jhon Doe'), (111,'Philosophy','Prof. Jhon Doe'), 
(112,'Art','Prof. Jhon Doe'), (102,'Music','Prof. Jhon Doe');


select * from courses8982860;


--15. Create a partitioned table named students_partitioned with columns id, name, and age,
--partitioned by grade.


CREATE TABLE IF NOT EXISTS students_partitioned8982860(
id int,
name string,
age int
)
PARTITIONED BY(grade string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

insert into students_partitioned8982860 (id, name, age, grade) values (1, 'Mort Teck', 23, 'A'),
(2, 'Caddric Kupisz', 24, 'B'), (3, 'Rakel Ormston', 20, 'C'),
(4, 'Hertha O'' Lone', 20, 'C'), (5, 'Raphaela Waiting', 24, 'A'),
(6, 'Janot Betterton', 25, 'C'),(7, 'Rickard Cubley', 24, 'A'),
(8, 'Jayson Thebes', 23, 'C'),(9, 'Bianka Manoelli', 22, 'C'),
(10, 'Laurette Carverhill', 22, 'C'),(11, 'Ahmad Stapele', 23, 'B'),
(12, 'Nisse Peret', 25, 'B'),(13, 'Willa Norkett', 20, 'A');

select * from students_partitioned8982860;

SHOW PARTITIONS students_partitioned8982860;

--17. Create a bucketed table named students_bucketed with columns id, name, age, and
--grade, clustered by id into 3 buckets.

students_bucketed

CREATE TABLE students_bucketed8982860(
id int,
name string,
age int)
PARTITIONED BY(grade string)
CLUSTERED BY (id) INTO 3 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';


insert into students_bucketed8982860 (id, name, age, grade) values (1, 'Mort Teck', 23, 'A'),
(2, 'Caddric Kupisz', 24, 'B'), (3, 'Rakel Ormston', 20, 'C');

select * from students_bucketed8982860;




CREATE TABLE IF NOT EXISTS students_new8982860(
id int,
name string,
age int,
grade string
) ROW FORMAT DELIMITED FIELDS TERMINATED
BY ',';

select * from students_new8982860;

CREATE TABLE IF NOT EXISTS courses_new8982860(
coursed int,
course_name string,
instructor_name string
)ROW FORMAT DELIMITED FIELDS TERMINATED
BY ',';

select * from courses_new8982860;

LOAD DATA LOCAL INPATH '/opt/hive/host_data/data_db/students.csv' OVERWRITE INTO TABLE students_new8982860;

LOAD DATA LOCAL INPATH '/opt/hive/host_data/data_db/courses.csv' OVERWRITE INTO TABLE courses_new8982860;


USE test_db;

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

DROP TABLE orders8982860;

CREATE TABLE IF NOT EXISTS orders8982860(
order_id int,
customer_id int,
order_date DATE,
amount float
)
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

-- customersxxx Table Insert:
INSERT INTO customers8982860 VALUES (1, 'Alice', 30, 'alice@example.com'),
(2, 'Bob', 25, 'bob@example.com'),
(3, 'Charlie', 35, 'charlie@example.com'),
(4, 'David', 28, 'david@example.com'),
(5, 'Emma', 40, 'emma@example.com'),
(6, 'Frank', 22, 'frank@example.com'),
(7, 'Grace', 27, 'grace@example.com'),
(8, 'Hannah', 32, 'hannah@example.com'),
(9, 'Ian', 26, 'ian@example.com'),
(10, 'Jack', 29, 'jack@example.com'),
(11, 'Karen', 33, 'karen@example.com'),
(12, 'Leo', 24, 'leo@example.com'),
(13, 'Mona', 37, 'mona@example.com');

SELECT * FROM customers8982860;


--Ordersxxx Table Insert:
INSERT INTO orders8982860 VALUES (1, 1, '2024-01-01', 100.0),
(2, 1, '2024-02-01', 150.0),
(3, 2, '2024-01-15', 200.0),
(4, 3, '2024-03-01', 50.0),
(5, 4, '2024-01-20', 300.0),
(6, 5, '2024-02-15', 400.0),
(7, 6, '2024-03-10', 250.0),
(8, 7, '2024-01-25', 350.0),
(9, 8, '2024-02-20', 150.0),
(10, 9, '2024-03-05', 100.0),
(11, 10, '2024-01-10', 200.0),
(12, 11, '2024-02-05', 150.0),
(13, 12, '2024-03-15', 100.0);

SELECT * FROM orders8982860;



SELECT * FROM customers8982860;

INSERT INTO customers8982860 VALUES (14, 'Mr.Rogers', 31,'mrogers@example.com');

SELECT * FROM customers8982860 WHERE customer_id = 14;

--Update the email of customer_id 5 to 'emma.new@example.com'.


UPDATE customers8982860 SET 
email = 'emma.new@example.com'
WHERE customer_id = 5;

SELECT * FROM customers8982860 WHERE customer_id = 5;

--customer record with customer_id 7.

DELETE FROM customers8982860 WHERE customer_id = 7;


SELECT 
o.order_id ,
c.name ,
o.order_date,
o.amount 
FROM orders8982860 o 
INNER JOIN customers8982860 c ON c.customer_id  = o.customer_id 

SELECT 
customer_id ,SUM(amount) as Total 
FROM orders8982860
GROUP BY customer_id;

SELECT * FROM customers8982860;


SELECT AVG(age) avg_age FROM customers8982860;

SELECT name, CASE WHEN age > 30  THEN 'age is equal to avg' ELSE 'age is above the average' END as flag FROM customers8982860;

add_months
SELECT order_date  FROM orders8982860;

SELECT * FROM orders8982860;


SELECT add_months(order_date,3)  FROM orders8982860;

SELECT * FROM orders8982860;

UPDATE orders8982860 SET 
order_date = add_months(order_date,3)

SELECT * FROM orders8982860
WHERE amount = (SELECT MAX(amount) FROM orders8982860);

SELECT * FROM orders8982860;

SELECT o.order_id, c.name, (SELECT MAX(amount) FROM orders8982860) as higths_amount,  o.amount,
((SELECT MAX(amount) FROM orders8982860) - o.amount) as diff
FROM orders8982860 o
INNER JOIN customers8982860 c ON c.customer_id  = o.customer_id;


--Create a partitioned table named orders_partitioned with columns order_id,
--customer_id, order_date, and amount, partitioned by customer_id

DROP TABLE orders_partitioned8982860

CREATE TABLE IF NOT EXISTS orders_partitioned8982860(
order_id int,
order_date DATE,
amount float
)
PARTITIONED BY(customer_id int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';


INSERT INTO orders_partitioned8982860(order_id, customer_id,order_date, amount) VALUES
(14, 8, '2024-03-20', 500.0),
(15, 9, '2024-04-01', 600.0),
(16, 10, '2024-02-28', 700.0),
(17, 11, '2024-03-30', 800.0),
(18, 12, '2024-01-25', 900.0),
(19, 13, '2024-04-15', 1000.0);

SELECT * FROM orders_partitioned8982860;


