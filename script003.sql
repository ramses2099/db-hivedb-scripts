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
