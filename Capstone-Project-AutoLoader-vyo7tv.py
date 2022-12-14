# Databricks notebook source
# MAGIC %md
# MAGIC ## DS-2002: Capstone Project
# MAGIC #### Name: Victoria Ok (vyo7tv)
# MAGIC Notebook for the DS-2002 capstone project. This data project builds upon the first project, implementing data science systems such as relational/NoSQL databases and ETL process pipelines, while also utilizing (attempting to use) cloud services that can support a data lakehouse architecture.
# MAGIC 
# MAGIC This project uses the [Sakila](https://dev.mysql.com/doc/sakila/en/) database, which represents a business process of the interaction between customers and retailers (staff) that is reflected in movie rental transactions.
# MAGIC 
# MAGIC **Systems Used Include:**
# MAGIC - Relational Database Management Systems (MySQL, Microsoft Azure SQL Server)
# MAGIC   - Online Transaction Processing Systems (OLTP): *Relational Databases Optimized for High-Volume Write Operations; Normalized to 3rd Normal Form.*
# MAGIC   - Online Analytical Processing Systems (OLAP): *Relational Databases Optimized for Read/Aggregation Operations; Dimensional Model (i.e, Star Schema)*
# MAGIC - NoSQL Systems (MongoDB Atlas)
# MAGIC - File System *(Data Lake)* Source Systems (Microsoft Azure Data Lake Storage)
# MAGIC   - Various Datafile Formats (e.g., JSON, CSV)
# MAGIC - Massively Parallel Processing *(MPP)* Data Integration Systems (Apache Spark, Databricks)
# MAGIC - Data Integration Patterns (e.g., Extract-Transform-Load, Extract-Load-Transform, Extract-Load-Transform-Load, Lambda & Kappa Architectures)
# MAGIC 
# MAGIC ### Section I: Prerequisites
# MAGIC 
# MAGIC #### 1.0. Import Required Libraries
# MAGIC 
# MAGIC Libraries are also installed to my cluster connected to this notebook:
# MAGIC - org.mongodb.spark:mongo-spark-connector_2.12:3.0.1
# MAGIC - pymongo[srv]

# COMMAND ----------

import os
import json
import pymongo
import pyspark.pandas as pd  # This uses Koalas that is included in PySpark version 3.2 or newer.
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType
from pyspark.sql.types import ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Instantiate Global Variables
# MAGIC 
# MAGIC These variables contain information for connections to other services (MongoDB Atlas, Azure SQL Server), as well as directory paths for where data files (CSV/JSON) are located.

# COMMAND ----------

# Azure SQL Server Connection Information #####################
jdbc_hostname = "ds-2002-mysql-vok.mysql.database.azure.com" # server name of my created Azure Database for MySQL flexible server
jdbc_port = 3306 # had also tried the default 1433 but it also did not work
src_database = "sakilaLT" # the name of my database in the MySQL flexible server!!

# user and password for the server I created
# note: not entirely sure if this driver is correct
connection_properties = {
  "user" : "vok",
  "password" : "Password123",
  "driver" : "org.mariadb.jdbc.Driver"
}

# MongoDB Atlas Connection Information ########################
# created a MongoDB Atlas cluster and database inside it, as well as a user that has read/write access to it
atlas_cluster_name = "sandbox"
atlas_database_name = "sakila"
atlas_user_name = "m01-user"
atlas_password = "y9p2O0OG65AhtC0h"

# Data Files (JSON) Information ###############################
dst_database = "sakila1"

# the file path to where data files are located
base_dir = "dbfs:/FileStore/ds2002-capstone"
database_dir = f"{base_dir}/{dst_database}"

data_dir = f"{base_dir}/source_data"
batch_dir = f"{data_dir}/batch"
stream_dir = f"{data_dir}/stream"

output_bronze = f"{database_dir}/fact_rentals_orders/bronze"
output_silver = f"{database_dir}/fact_rentals_orders/silver"
output_gold   = f"{database_dir}/fact_rentals_orders/gold"

# Delete the Streaming Files ################################## 
dbutils.fs.rm(f"{database_dir}/fact_rentals_orders", True)

# Delete the Database Files ###################################
dbutils.fs.rm(database_dir, True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Define Global Functions

# COMMAND ----------

# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the Azure SQL database server.
# ######################################################################################################################
def get_sql_dataframe(host_name, port, db_name, conn_props, sql_query):
    '''Create a JDBC URL to the Azure SQL Database'''
    jdbcUrl = f"jdbc:mysql://{host_name}:{port}/{db_name}"
    
    '''Invoke the spark.read.jdbc() function to query the database, and fill a Pandas DataFrame.'''
    dframe = spark.read.jdbc(url=jdbcUrl, table=sql_query, properties=conn_props)
    
    return dframe


# ######################################################################################################################
# Use this Function to Fetch a DataFrame from the MongoDB Atlas database server Using PyMongo.
# ######################################################################################################################
def get_mongo_dataframe(user_id, pwd, cluster_name, db_name, collection, conditions, projection, sort):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.mmkfa54.mongodb.net/{db_name}"
    
    client = pymongo.MongoClient(mongo_uri)

    '''Query MongoDB, and fill a python list with documents to create a DataFrame'''
    db = client[db_name]
    if conditions and projection and sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection).sort(sort)))
    elif conditions and projection and not sort:
        dframe = pd.DataFrame(list(db[collection].find(conditions, projection)))
    else:
        dframe = pd.DataFrame(list(db[collection].find()))

    client.close()
    
    return dframe

# ######################################################################################################################
# Use this Function to Create New Collections by Uploading JSON file(s) to the MongoDB Atlas server.
# ######################################################################################################################
def set_mongo_collection(user_id, pwd, cluster_name, db_name, src_file_path, json_files):
    '''Create a client connection to MongoDB'''
    mongo_uri = f"mongodb+srv://{user_id}:{pwd}@{cluster_name}.mmkfa54.mongodb.net/{db_name}"
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    
    '''Read in a JSON file, and Use It to Create a New Collection'''
    for file in json_files:
        db.drop_collection(file)
        json_file = os.path.join(src_file_path, json_files[file])
        with open(json_file, 'r') as openfile:
            json_object = json.load(openfile)
            file = db[file]
            result = file.insert_many(json_object)

    client.close()
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section II: Populate Dimensions by Ingesting Reference (Cold-path) Data 
# MAGIC #### 1.0. Fetch Reference Data From an Azure SQL Database
# MAGIC ##### 1.1. Create a New Databricks Metadata Database, and then Create a New Table that Sources its Data from a View in an Azure SQL database.
# MAGIC 
# MAGIC The code block below will drop an existing database with the name ```sakila1```

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS sakila1 CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC The code block below will create a database with the name ```sakila1``` and place it at the specified file path.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sakila1
# MAGIC COMMENT "Capstone Project Database"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/sakila1"
# MAGIC WITH DBPROPERTIES (contains_pii = true, purpose = "DS-2002 Capstone Project");

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_customers
# MAGIC 
# MAGIC The code below throws an exception:
# MAGIC 
# MAGIC ```com.microsoft.sqlserver.jdbc.SQLServerException: The TCP/IP connection to the host ds-2002-mysql-vok.mysql.database.azure.com, port 3306 has failed. Error: &#34; The driver received ```
# MAGIC ```an unexpected pre-login response. Verify the connection properties and check that an instance of SQL Server is running on the host and accepting TCP/IP connections at the port.``` 
# MAGIC ```This driver can be used only with SQL Server 2005 or later.&#34;. ClientConnectionId:37d664c6-0fb3-4eca-ac9d-69f16342bf1c```
# MAGIC 
# MAGIC My connection properties (TCP/IP) do not seem to be working, but this SQL query will create a temporary view (a virtual table whose data consists of the output of SQL queries) called ```view_customers``` and will fill the view with the data from the ```dim_customers``` table from my ```sakilaLT``` database hosted on the SQL server (or it will replace an existing temporary view with the same name). Because it is a temporary view, this view will onyl be visible during the session it was created (to view it again you have to rerun the query in a new session).
# MAGIC 
# MAGIC Note: I had also tried using the default port 1433, but it threw the same error. I also checked my firewall rules, and made it available everywhere (```0.0.0.0-255.255.255.255```)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_customers
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://ds-2002-mysql-vok.mysql.database.azure.com:3306;databaseName=sakilaLT",
# MAGIC   dbtable "dim_customers",
# MAGIC   user "vok",
# MAGIC   password "Password123"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC The query below throws an exception:
# MAGIC ```AnalysisException: Table or view not found: view_customers; line 4 pos 17;```
# MAGIC 
# MAGIC Because my temporary view above did not work, I am unable to populate the table being created (```dim_customers```). This query first uses the ```sakila1``` database in the Databricks metadata database. Then creates a table ```dim_customers``` that would be located at the file path given, populating it with the data from the view created above using a ```SELECT``` statement on this view.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE sakila1;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS sakila1.dim_customers
# MAGIC COMMENT "Customer Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/sakila1/dim_customers"
# MAGIC AS SELECT * FROM view_customers

# COMMAND ----------

# MAGIC %md
# MAGIC These next two queries display the newly created table. The first one shows the first 5 rows from the resulting query from ```dim_customers```, which displays information from every column. The second query returns a summary of this table, which includes column information (including its data type)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila1.dim_customers LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila1.dim_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.2. Create a New Table that Sources its Data from a Table in an Azure SQL database. 
# MAGIC 
# MAGIC ##### dim_date

# COMMAND ----------

# MAGIC %md
# MAGIC The SQL query below threw an exception:
# MAGIC 
# MAGIC ```com.microsoft.sqlserver.jdbc.SQLServerException: The TCP/IP connection to the host ds-2002-mysql-vok.mysql.database.azure.com, port 1433 has failed. Error: &#34;connect timed out. Verify the ``` ```connection properties. Make sure that an instance of SQL Server is running on the host and accepting TCP/IP connections at the port. Make sure that TCP connections to the port are not blocked ```
# MAGIC ```by a firewall.&#34;.```
# MAGIC 
# MAGIC My connection properties (TCP/IP) do not seem to be working, however, similar to the customer view, this SQL query will create a temporary view called ```view_date``` and will fill the view with the data from the ```DimDate``` table from my ```dbo``` database hosted on the SQL server (or it will replace an existing temporary view with the same name). 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW view_date
# MAGIC USING org.apache.spark.sql.jdbc
# MAGIC OPTIONS (
# MAGIC   url "jdbc:sqlserver://ds-2002-mysql-vok.mysql.database.azure.com:1433;database=SakilaLT",
# MAGIC   dbtable "dbo.DimDate",
# MAGIC   user "vok",
# MAGIC   password "Password123"
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC The query below threw an exception: 
# MAGIC ```AnalysisException: Table or view not found: view_date; line 4 pos 17;```
# MAGIC 
# MAGIC Because my temporary view above did not work, I am unable to populate the table being created (```dim_date```). This query first uses the ```sakila1``` database in the Databricks metadata database. Then creates a table ```dim_date``` that would be located at the file path given, populating it with the data from the view created above using a ```SELECT``` statement on this view.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE sakila1;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS sakila1.dim_date
# MAGIC COMMENT "Date Dimension Table"
# MAGIC LOCATION "dbfs:/FileStore/ds2002-capstone/sakila1/dim_date"
# MAGIC AS SELECT * FROM view_date

# COMMAND ----------

# MAGIC %md
# MAGIC These next two queries display the newly created table. The first one shows the first 5 rows from the resulting query from ```dim_date```, which displays information from every column. The second query returns a summary of this table, which includes column information (including its data type)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila1.dim_date LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila1.dim_date;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.0. Fetch Reference Data from a MongoDB Atlas Database
# MAGIC ##### 2.1. View the Data Files on the Databricks File System

# COMMAND ----------

# outputs all files in this directory, with information on the file path, file name, size, and modification time
display(dbutils.fs.ls(batch_dir))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.2. Create a New MongoDB Database, and Load JSON Data Into a New MongoDB Collection
# MAGIC **NOTE:** The following cell **can** be run more than once because the **set_mongo_collection()** function **is** idempotent.
# MAGIC 
# MAGIC After viewing all the files in the directory from above, I use one of these JSON files to populate a MongoDB Atlas database.
# MAGIC 
# MAGIC ##### dim_inventory

# COMMAND ----------

source_dir = '/dbfs/FileStore/ds2002-capstone/source_data/batch'
json_files = {"inventory" : 'dim_inventory.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas_database_name, source_dir, json_files) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_customers
# MAGIC Note: because I was not able to extract the customer data from a SQL database, I am extracting the data through JSON.

# COMMAND ----------

source_dir = '/dbfs/FileStore/ds2002-capstone/source_data/batch'
json_files = {"customers" : 'dim_customers.json'}

set_mongo_collection(atlas_user_name, atlas_password, atlas_cluster_name, atlas_database_name, source_dir, json_files) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.3. Fetch Data from the New MongoDB Collection
# MAGIC 
# MAGIC ##### dim_inventory
# MAGIC 
# MAGIC Using Apache Spark, aftering specifying that the data be inserted into the ```sakila``` database on my Atlas cluster into a table named ```inventory```, I can view this new NoSQL table I created.

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC 
# MAGIC val df_inventory = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "sakila").option("collection", "inventory").load()
# MAGIC display(df_inventory)

# COMMAND ----------

# MAGIC %md
# MAGIC With the ```printSchema()``` function, I can output the schema information for this table ```inventory```, which includes each column name, its type, and whether it can be null or not.

# COMMAND ----------

# MAGIC %scala
# MAGIC df_inventory.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_customers

# COMMAND ----------

# MAGIC %scala
# MAGIC import com.mongodb.spark._
# MAGIC 
# MAGIC val df_customers = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("database", "sakila").option("collection", "customers").load()
# MAGIC display(df_customers)

# COMMAND ----------

# MAGIC %scala
# MAGIC df_customers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.4. Use the Spark DataFrame to Create a New Table in the Databricks (Sakila) Metadata Database
# MAGIC 
# MAGIC ##### dim_inventory
# MAGIC 
# MAGIC The function below uses the ```df_inventory``` dataframe to write data into the ```sakila1``` database in the Databricks Metadata Database, overwriting information is applicable

# COMMAND ----------

# MAGIC %scala
# MAGIC df_inventory.write.format("delta").mode("overwrite").saveAsTable("sakila1.dim_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC To ensure that the data was inserted correctly, we can query the table with ```DESCRIBE EXTENDED``` to get a summary of the table, including the column names and their data types.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila1.dim_inventory

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_customers

# COMMAND ----------

# MAGIC %scala
# MAGIC df_customers.write.format("delta").mode("overwrite").saveAsTable("sakila1.dim_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila1.dim_customers

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2.5. Query the New Table in the Databricks Metadata Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila1.dim_inventory LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila1.dim_customers LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.0. Fetch Data from a File System
# MAGIC ##### 3.1. Use PySpark to Read From a CSV File
# MAGIC 
# MAGIC ##### dim_staff

# COMMAND ----------

# set the file path of the CSV file
staff_csv = f"{batch_dir}/dim_staff.csv"

# Using Apache Spark, read this file in the CSV format, taking in the fact that the first row will be column names, and to infer the data types/nullability
df_staff = spark.read.format('csv').options(header='true', inferSchema='true').load(staff_csv)
# display the contents of the file
display(df_staff)

# COMMAND ----------

# output the column names, data types, and nullability
df_staff.printSchema()

# COMMAND ----------

# Write the data from this dataframe into the 'sakila1' database in the Databricks Metadata Database with the table name being 'dim_staff' (overwriting if applicable)
df_staff.write.format("delta").mode("overwrite").saveAsTable("sakila1.dim_staff")

# COMMAND ----------

# MAGIC %md
# MAGIC Query this new table from the Databricks Metadata Database to ensure that all the data was inserted properly

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila1.dim_staff;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila1.dim_staff LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dim_date
# MAGIC Note: because I was not able to extract the customer data from a SQL database, I am extracting the data through JSON.

# COMMAND ----------

# set the file path of the CSV file
date_csv = f"{batch_dir}/dim_date.csv"

# Using Apache Spark, read this file in the CSV format, taking in the fact that the first row will be column names, and to infer the data types/nullability
df_date = spark.read.format('csv').options(header='true', inferSchema='true').load(date_csv)
# display the contents of the file
display(df_date)

# COMMAND ----------

# output the column names, data types, and nullability
df_date.printSchema()

# COMMAND ----------

# Write the data from this dataframe into the 'sakila1' database in the Databricks Metadata Database with the table name being 'dim_staff' (overwriting if applicable)
df_date.write.format("delta").mode("overwrite").saveAsTable("sakila1.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Query this new table from the Databricks Metadata Database to ensure that all the data was inserted properly

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila1.dim_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sakila1.dim_date LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Verify Dimension Tables
# MAGIC 
# MAGIC Use a SQL query is display all the tables in the ```sakila1``` database, with extra information on whether the table is temporary (meaning will it disappear after the current session ends)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE sakila1;
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Section III: Integrate Reference Data with Real-Time Data
# MAGIC #### 6.0. Use AutoLoader to Process Streaming (Hot Path) Data 
# MAGIC ##### 6.1. Bronze Table: Process 'Raw' JSON Data
# MAGIC 
# MAGIC From the streaming table directory, create (or replace) a temporary view called ```rentals_raw_tempview``` by auto loading JSON files through a structured streaming source (```cloudFiles```). Schema information is also provided, with column names and data types.

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaHints", "rental_key INT")
 .option("cloudFiles.schemaHints", "customer_key INT")
 .option("cloudFiles.schemaHints", "staff_key INT")
 .option("cloudFiles.schemaHints", "store_key INT")
 .option("cloudFiles.schemaHints", "inventory_key INT")
 .option("cloudFiles.schemaHints", "rental_date DATETIME")
 .option("cloudFiles.schemaHints", "return_date DATETIME")
 .option("cloudFiles.schemaHints", "modified_date TIMESTAMP")
 .option("cloudFiles.schemaLocation", output_bronze)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("multiLine", "true")
 .load(stream_dir)
 .createOrReplaceTempView("rentals_raw_tempview"))

# COMMAND ----------

# MAGIC %md
# MAGIC Create another temporary view called ```rentals_bronze_tempview``` that contains all the information from the temporary view just created (```rentals_raw_tempview```), in addition to extra information:
# MAGIC - ```receipt_time```: the current timestamp
# MAGIC - ```source_file```: the file path from where the data came from

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Add Metadata for Traceability */
# MAGIC CREATE OR REPLACE TEMPORARY VIEW rentals_bronze_tempview AS (
# MAGIC   SELECT *, current_timestamp() receipt_time, input_file_name() source_file
# MAGIC   FROM rentals_raw_tempview
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Query this new temporary view ```rentals_bronze_tempview``` to ensure that the data was inserted correctly

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rentals_bronze_tempview

# COMMAND ----------

# MAGIC %md
# MAGIC Because the data from the temporary view was from streaming data, we want to append this data to an existing fact table - ```fact_rentals_bronze``` (the table that represents the business transaction: movie rentals between customers and retail staff)

# COMMAND ----------

(spark.table("rentals_bronze_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_bronze}/_checkpoint")
      .outputMode("append")
      .table("fact_rentals_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.2. Silver Table: Include Reference Data
# MAGIC 
# MAGIC The Bronze table contained just the raw data, but we want to put this data in context, so in the silver table, we include reference data, such as the customer's full name and email, rather than just the customer's ID (key)
# MAGIC 
# MAGIC Use Apache Spark to create a temporary view (or replace) ```rentals_silver_tempview``` that contains all the information from the ```fact_rentals_bronze``` table.

# COMMAND ----------

(spark.readStream
  .table("fact_rentals_bronze")
  .createOrReplaceTempView("rentals_silver_tempview"))

# COMMAND ----------

# MAGIC %md
# MAGIC Query this view to ensure that all the data from ```fact_rentals_bronze``` was inserted properly

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rentals_silver_tempview

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED rentals_silver_tempview

# COMMAND ----------

# MAGIC %md
# MAGIC Create another temporary view (or replace it if it already exists) ```fact_rentals_silver_tempview``` that has all the information from the view created above (```rentals_silver_tempview```), but add all the reference data by joining the view with tables from the ```sakila1``` database from the Databricks Metadata Database: ```dim_customers```, ```dim_staff```, ```dim_inventory```, and ```dim_date```. The date is split up to provide a more detailed view of the data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW fact_rentals_silver_tempview AS (
# MAGIC   SELECT t.rental_key
# MAGIC     , c.customer_key
# MAGIC     , c.first_name
# MAGIC     , c.last_name
# MAGIC     , c.email
# MAGIC     , s.staff_key
# MAGIC     , s.first_name
# MAGIC     , s.last_name
# MAGIC     , s.email
# MAGIC     , s.store_key
# MAGIC     , i.inventory_key
# MAGIC     , i.title
# MAGIC     , i.release_year
# MAGIC     , i.rental_duration
# MAGIC     , i.rental_rate
# MAGIC     , i.length
# MAGIC     , i.replacement_cost
# MAGIC     , i.rating
# MAGIC     , od.MonthName AS rental_month
# MAGIC     , od.WeekDayName AS rental_day_name
# MAGIC     , od.Day AS rental_day
# MAGIC     , od.Year AS rental_year
# MAGIC     , dd.MonthName AS return_month
# MAGIC     , dd.WeekDayName AS return_day_name
# MAGIC     , dd.Day AS return_day
# MAGIC     , dd.Year AS return_year
# MAGIC     , t.modified_date
# MAGIC     , t.receipt_time
# MAGIC     , t.source_file
# MAGIC   FROM rentals_silver_tempview t
# MAGIC   INNER JOIN sakila.dim_customer c
# MAGIC   ON t.CustomerID = c.CustomerID
# MAGIC   INNER JOIN sakila.staff s
# MAGIC   ON t.StaffID = s.StaffID
# MAGIC   INNER JOIN sakila.inventory i
# MAGIC   ON t.InventoryID = i.InventoryID
# MAGIC   INNER JOIN sakila.dim_date od
# MAGIC   ON CAST(t.RentalDate AS DATE) = od.Date
# MAGIC   INNER JOIN sakila.dim_date dd
# MAGIC   ON CAST(t.ReturnDate AS DATE) = dd.Date)

# COMMAND ----------

# MAGIC %md
# MAGIC Because the data from the temporary view was from streaming data, we want to append this data to an existing fact table - ```fact_rentals_silver``` (the table that represents the business transaction: movie rentals between customers and retail staff, with more detailed information about the entities involved in the transactions)

# COMMAND ----------

(spark.table("fact_rentals_silver_tempview")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{output_silver}/_checkpoint")
      .outputMode("append")
      .table("fact_rentals_silver"))

# COMMAND ----------

# MAGIC %md
# MAGIC Query this updated fact table ```fact_rentals_silver`` to ensure that the data was inserted correctly

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fact_rentals_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED sakila1.fact_rentals_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6.4. Gold Table: Perform Aggregations
# MAGIC 
# MAGIC Perform aggregations to analyze the data appropriately - what about this business transaction is important? 
# MAGIC 
# MAGIC This query displays each customer's last name, how many times they've rented out movies, and the total amount they spent on rentals during the span of time this data was collected. It is ordered in descending order of the total amount spent on rentals. The resulting output is valuable for the retailers because with this information, they can see objectively who are their most frequent customers, which can open more converstaions for future business endeavors or initiatives, such as a member rewards program.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.last_name AS customer_name,
# MAGIC       COUNT(i.inventory_key) AS total_rental_quantity,
# MAGIC       SUM(i.rental_rate) AS total_rental_price
# MAGIC FROM sakila1.fact_rentals_silver AS rentals
# MAGIC INNER JOIN sakila1.dim_customers AS c
# MAGIC ON rentals.customer_key = c.customer_key
# MAGIC INNER JOIN sakila1.dim_inventory AS i
# MAGIC ON rentals.inventory_key = i.inventory_key
# MAGIC GROUP BY c.last_name
# MAGIC ORDER BY total_rental_price DESC;
