# Interview-70

**Topics:** PySpark | Python | DataBricks | SQL

---

1. Explain about your project.
2. What data quality checks you did in your project?
3. How data quality was done in your project? Was there any tool used for it?
4. What were various optimizations done in your project?
5. Share any issue which you resolve in your previous projects.
6. How will you handle incremental data in your project? How to make sure only incremental data is processed?
7. What is difference between group by and window function?
8. What were various transformations done in your project and for what purposes they were used?
9. How did you perform data validation at various transformation steps in your project?
10. Did your project use any type of data governance mechanism?
11. What were some minor changes done in the project that led to increase in performance in your project?
12. How will you handle changing schema of your input data?

---

# Interview-71

**Interviewer:** shrivishnu and vishwas

**Interviewee:** Shruti Dhage

Hi I have recently attended interview for swiss re project and cleared it. Most of the questions are already covered in KB page. I have not faced any new questions. However for your ease of understanding and getting selected for swiss re i have created this attached document. This contains all the questions along with answers which i had faced or others faced. Please go through it in case if you have interview with Swiss re. All the best!

**Attachment:** SwissRe Interview.docx

**Note:** Project related questions are not covered in this document as the answers may variate depending on the project.

---

# Interview-72

**Topics:** Java, SQL, Data Engineering, GCP

---

1. Java program to validate if array_1 contains in array_2 or not in same order.

   For example,

   a = {1,2,3,4,5,6}

   b = {2,3,4}

   Output = Yes

   b = {2,4,5}

   Output = No

2. SQL query to self-join on employee (emp_id, name, manager_id) table to list employee nd his manager's name.
3. Hive tow tables (Product and Category), how many tow as output for inner_join, Left_join, Right_Join and Cross_Join
4. Design "Vanding machine" database tables and their relationship.
5. Design DB tables for employee staffing Mangement system. Three tables Employee, Projects, assignments

   Based on above tables design, various sql queries:
   - Total employee in each project?
   - Total active projects?
   - Employee working in multiple projects?
6. GCP BigQuery optimization, Partitioning, Cost analysis, Table Sharding, Materialized View, Caching etc.
7. SQL Query optimization techniques
8. Hive: Internal vs External tables, Partitions
9. Delete vs drop vs truncate command

---

# Interview-73

**Topics:** Python, Pyspark, SQL, Data Engineering

---

1. l1 = [a,b,c] , l2 = [10,5,20]

   Write a python program to sort first list based on second list. Achieve this in pyspark as well.

   o/p = [b,a,c]

2. l = [0,1,1,2,3,3,4]

   Any 3 ways to remove duplicates from the above list. o/p: [0,1,2,3,4]

   Follow up question: completely remove the duplicated value. o/p: [0,2,4]

3. Table:
   ```
   emp_id,salary,year
   1,10,2019
   1,8,2018
   1,7,2017
   2,100,2019
   2,90,2018
   2,95,2017
   ```

   Write a sql query to identify the emp_id whose salary is consistently increasing each year.

   o/p:
   ```
   emp_id
   1
   ```

4. Table:
   ```
   emp_id,area,distance,flag
   A,x,10,Y
   A,x,20,Y
   A,x,30,N
   B,p,10,N
   ```

   Write a pyspark code to add a new column with logic = Banglore.emp_name.area.distance
   (here 'Bangalore' is literal and emp_name, area, distance are dynamic values. Distance here should be max_distance where flag=Y)

   Expected o/p:
   ```
   emp_id,area,distance,flag,result
   A,x,10,Y,Bangalore.A.x.20
   A,x,20,Y,Bangalore.A.x.20
   A,x,30,N,Bangalore.A.x.20
   B,p,10,N,null
   ```

5. Read a json file and flatten using pyspark.
6. Optimization techniques in spark. (Talked about data skewness, data spill, partitioning, columnar storage file formats like parquet)
7. Approach of unit testing (what are the things that you will consider)
8. input = 1995-12-01 (birth date)

   Calculate age of the person. Do this in Python, Pyspark and SQL.
9. Talk about any spark optimization that you have done in recent times.
10. Approach of debugging the long running spark jobs.

---

# Interview-74

**Topics:** Spark

---

1. Introduction and about responsibilities in most recent project.
2. Spark Architecture
3. Spark Optimization techniques
4. Spark submit basic configurations
5. Commands - select from table 1, select from table 2, join both table, filter, groupBy, filter, saveAsTable. Based on the commands how many stages, jobs and tasks will be created.
6. What makes RDD fault tolerant
7. Why spark is extensively used in bug data world.
8. Unity catalog advantages
9. About delta lake
10. Dataframe with 6 months of data. 1 row is 1 week of data. Each row will have number of stocks of that week and number of stocks sold. Now number of stocks remaining from previous week needs to be added to the coming week. Using for loop for this but it is slow what can be done to make it faster.
11. Where exactly spark runs? inside JVM, where
12. How does the on-heap memory comes? Off-heap
13. Garbage collector comes into the picture?
14. Define class in python?
15. Define static and class method, give promotion to employees of 10%
16. Wal_level, what is write ahead logs? why we use that?
17. ACID -> What exactly is that? Each step? how isolation works in background?
18. What exactly is container in spark? why we require it.
19. Hadoop 1 vs Hadoop 2.
20. cluster vs standalone
21. resource manager, constraints, executors where exactly it is running
22. Serialization -> how object data is stored in spark? why it is even needed in spark
23. medallion architecture
24. unity category in details vs glue catalog vs hive catalog vs without catalog
25. Spark framework, submit command, Dag creation
26. Why Kryo is More Lightweight than Java Serialization?
27. spark memory management (chache vs persist) - types or persist.
28. databricks vs snowflake in terms of catalog (lake house vs data warehourse)
29. create a decorator in code.
30. difference between decorator vs generator vs iterator
31. row_number, rank and dens_rank
32. Write code in spark sql then in dataframe and include OOPs concept.

   Table:
   ```
   transaction_id,customer_id,transaction_date,amount,category
   1,101,2023-01-01,100,Food
   2,102,2023-01-02,200,Electronics
   3,101,2023-01-03,50,Food
   4,103,2023-01-01,300,Clothing
   5,102,2023-01-03,150,Food
   6,104,2023-01-05,120,Electronics
   7,101,2023-01-06,30,Food
   8,105,2023-01-01,500,Travel
   9,102,2023-01-04,80,Food
   10,103,2023-01-02,200,Clothing
   ```

   Queries to write:
   1. 3 top spending customer per category
   2. Top spending customer per category
   3. Daily spending percentile
   4. Customer ranking by total spend
   5. Cumulative spending per customer

---

# Interview-75

**Group:** A3-A4

**Topics:** Spark, Scala, SQL

---

## Section 1: Spark, Scala & SQL Concepts

1. Introduction and project?
2. What is shuffling in spark and when it happens?
3. Persist, cache in spark and what are the different storage levels. Does the data will spillover if using cache?
4. Predicate pushdown filter in spark and when it happens
5. Scala vs Java
6. What's the difference between 'Nil', 'null', 'None' and 'Nothing' in Scala?
7. What is delta table
8. What is fault tolerant in spark
9. Production issues in project
10. Why Scala can access Java libraries?
11. Scala garbage collection
12. What if the task1 failes in stage 3. From where it will take the data for reprocessing:

    ```
    stage1     stage2     stage3
    100/100    50/50      40/39 1fail
    ```

13. Given the below code, does it work?

    ```scala
    class A(f1: String, f2: String)

    var a = new A("1", "2")
    a.f1 = "v2"
    ```

14. How to find the columns of dataframe
15. 1000 files of each 50mb of below data (nationality:age). How to find avg age for nationality?

    ```
    us:56, .....
    ```

16. Given DS1, DS2 and the expected result below, write steps to get the result dataset which contains non null values.

    **DS1**
    ```
    id  name  salary  phone
    1   Bob   5000    333-555
    2   Rob   7000    null
    3   Snob  5000    null
    ```

    **DS2**
    ```
    id  name  salary  phone
    1   Bob   1000    null
    2   Rob   2000    777-666
    4   Nick  3000    444-333
    ```

    **Result**
    ```
    id  name  salary  Bunus  phone
    1   Bob   5000    1000   333-555
    2   Rob   7000    2000   777-666
    3   Snob  5000    null   null
    4   Nick  3000    3000   444-333
    ```

17. Write a function to take the nonnull columns of df1 and df2 after joining in spark scala
18. Describe below: `val`, `var`, `def`, `lazy val`
19. Spark internal working
20. What is AQE in Spark?
21. How many transformations in spark

---

## Section 2: Coding & Scenario Questions

**Q1.** Find the pairs from the list where sum is 13.

```python
from itertools import combinations
l1 = [2,11,9,4,5,8,6,10,3]

all_possible = combinations(l1,2)

for ele in all_possible:
    if ele[0] + ele[1] == 13:
        print(ele)
```

**Q2.** Create an external hive table, format as parquet, partititoned on date on below schema:

```
custid, tsn data, txn time, txn amt
```

**Q3.** Identify the customers who have avg spend per day > $50

**Q4.** Identify duplicate records from a table.

**Q5.** What is SCD 2, implement it in pyspark.

**Q6.** Suppose for a customer, the transaction value is loaded as NULL, for that cust replace null with $1000.

**Q7.** Airflow or NIFI — which to use when.

**Q8.** Spark optimization techniques used.

**Q9.** How to identify problem in spark jobs.

**Q10.** Data frame is mutable or not, why?

**Q11.** Source is csv data, load this data to hive that is partitioned on date column using spark.

---

## Section 3: Spark Coding Questions

Given `customer.csv` with schema:

```
c_id, f_name, city, state, country, dob
```

1. Validation: no additional columns & all columns should be there.
2. Identify null or empty values in all columns.
3. Create column name as `c_id_validation` with above validation values as "Failed" & "Passed".
4. If row exists with the value "Failed" have to generate exception and list down records.

---

## Section 4: Spark Scala Questions

1. Spark performance techniques?
2. How to define no of partitions to be created while creating repartition?
3. Scala case classes
