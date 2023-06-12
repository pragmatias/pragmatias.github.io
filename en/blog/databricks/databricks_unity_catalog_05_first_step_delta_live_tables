---
Categories : ["Databricks","Delta Live Table","Unity Catalog"]
Tags : ["Databricks","Delta Live Table","Unity Catalog"]
title : "Databricks : Unity Catalog - First Step - Part 4 - Delta Live Tables"
date : 2023-06-12
draft : false
toc: true
---

We are going to discover the [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) framework with the [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html) solution (instead of the Hive Metastore).

We're going to focus on the specific elements from the Unity Catalog solution.

The use of the Unity Catalog solution with the Delta Live Tables framework was in "Public Preview" at the time of writing this article in early June 2023.

We're going to use a Databricks account on AWS to make this discovery.

<!--more-->

# What's Delta Live Tables

## Overview

Delta Live Tables is a declarative framework for defining all the elements of an ETL processing pipeline.

This framework allows you to work in [Python](https://docs.databricks.com/delta-live-tables/python-ref.html) or [SQL](https://docs.databricks.com/delta-live-tables/sql-ref.html).

This framework lets you define objects in a Python script or in a Python or SQL notebook, which is then executed by a DLT (Delta Live Tables) pipeline using the "Workflows" functionality.

This framework is based on the following three types of objects to manage the orchestration of all the tasks in a DLT pipeline :
- **Streaming Table** : This corresponds to the SQL term `STREAMING TABLE <objet>`
- **Materialized View** : This corresponds to the SQL term `LIVE TABLE <objet>`
- **View** : This corresponds to the SQL term `TEMPORARY LIVE VIEW <objet>` or `TEMPORARY STREAMING LIVE VIEW <objet>`

You could find full details of costs and features available for each edition type with [this link](https://www.databricks.com/product/pricing/delta-live)
A concise overview of the different editions :
- DLT Core :
    - Allows you to manage basic functionality
- DLT Pro : 
    - Allows you to manage all the functions of the "DLT Core" edition, including Change Data Capture (CDC) and Slow Changing Dimension (SCD) type 2 object (very useful for simplifying referential data management when you want to keep track of changes)
- DLT Advanced :
  - Allows you to manage all the functions of the "DLT Pro" edition, as well as all data quality management functions (mainly the definition of constraints (expectations) and their observability (metrics)).


## Regarding objects

Regarding the **Streaming Table** object :
- This object can be used to manage the streaming of a data source and read a record only once (Spark Structured Streaming).
- Element management (checkpoint, etc.) is handled by default by the DLT framework.
- Data source must be append-only
- If the data source is managed with [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html), an additional column named `_rescued_data` will be created by default to handle malformed data.
- Expectations can be defined at object level
- To read a "Streaming table" object in the same DLT pipeline, use the syntax `STREAM(LIVE.<object>)` (SQL) or `dlt.read_stream("<object>")` (Python).
- It is possible to define the ingestion of data from a "Streaming Table" object with a type 2 Slow Changing Dimension process automatically managed by the DLT framework.
    - This makes it very easy to set up history management for referential data from a source using CDC (Change Data Capture) or CDF (Change Data Feed).
- Since objects are managed by the DLT framework when a DLT pipeline is executed, it's not recommended to changes data (update, delete, insert) outside the DLT pipeline.
    - It is possible to perform modifications (DML-type queries) with certain limitations on "Streaming Table" objects, but this is not recommended and not all operations are supported.

Regarding the **Materialized View** object :
- This object is used to manage the refresh of object data according to its state when the DLT pipeline is executed. The DLT framework defines what it should update according to the state of the sources and the target.
- Expectations can be defined at object level
- To read a "Materialized View" object in the same DLT pipeline, use the syntax `LIVE.<object>` (SQL) or `dlt.read("<object>")` (Python).
- It is not possible to execute DML (Update, Delete, Insert) queries on a "Materialized View" object, as the accessible object is not defined as a Delta table.
- To read a "Materialized View" object outside the DLT pipeline, you need to use a Shared Cluster or a SQL Warehouse Pro or Serverless (recommended).

Regarding the **View** object :
- This object is used to define a temporary view (encapsulated query) of a data source, which will only exist during execution of the DLT pipeline.
- The encapsulated query is executed each time the object is read
- Expectations can be defined at object level




## Regarding data quality management

Data quality management is done by declaring constraints (expectations) on objects:
- SQL Syntax : `CONSTRAINT expectation_name EXPECT (expectation_expr) [ON VIOLATION { FAIL UPDATE | DROP ROW }]`

When you define a constraint (expectation) on an object, the DLT framework will check when ingesting the record and perform the defined action :
- `CONSTRAINT expectation_name EXPECT (expectation_expr) ` : When there is no action defined, the records not respecting the constraint will be inserted in the object and information will be added in the event logs of the DLT pipeline with the number of records concerned for this object.
- `CONSTRAINT expectation_name EXPECT (expectation_expr) ON VIOLATION FAIL UPDATE` : When the action defined is "FAIL UPDATE", the DLT pipeline will stop in error at the first record not respecting the constraint with the error message in the event logs of the DLT pipeline.
- `CONSTRAINT expectation_name EXPECT (expectation_expr) ON VIOLATION DROP ROW` : When the action defined is "DROP ROW", the records that do not respect the constraint will not be inserted into the object and information will be added in the event logs of the DLT pipeline with the number of records concerned for this object.


## Regarding DLT pipeline

In order to be able to run a script (Python) or notebook (SQL or Python) with the DLT framework, it is necessary to create a DLT pipeline specifying the desired edition.

The definition of the DLT pipeline is done using the "Workflows" functionality.
Access is as follows :
1. Click on the "Data Science & Engineering" view in the sidebar
2. Click on "Workflows" option in the sidebar
3. Click on the "Delta Live Tables" tab
[![schema_01](/blog/web/20230612_databricks_unity_catalog_deltalivetables_01.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_01.png)

From this screen, you will be able to manage DLT pipelines (creation, deletion, configuration, modification) and view the different executions (within the limit of the observability data retention period defined for each pipeline)
[![schema_02](/blog/web/20230612_databricks_unity_catalog_deltalivetables_02.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_02.png)

Useful information for creating a DLT pipeline:
- General :
    - Product edition : Edition to be chosen among DLT Core, DLT Pro and DLT Advanced, this will define the usable functionalities as well as one of the criteria of the cost of the DLT pipeline execution
    - Pipeline mode : 
        - Triggered : Allows to manage the execution in batch mode and therefore to stop the cluster after execution of the DLT pipeline
        - Continuous : Allows to manage the execution in streaming mode and therefore to execute the pipeline continuously to process the data as soon as it arrives.
- Source Code : Allows to define the script (Python) or the Notebook (Python or SQL) to be executed by the DLT pipeline
- Destination
    - Storage Option : Choose Unity Catalog to use the Unity Catalog solution with the DLT framework
    - Catalog : Define the target catalog for the DLT pipeline (catalog that contains the target schema of the DLT pipeline)
    - Target schema : Define the target schema for the DLT pipeline (schema that will be used to manage DLT pipeline objects)
- Compute
    - Cluster mode : To define whether you want a fixed number of workers (Fixed size) or which adapts to the workload within the limits indicated (Enhanced autoscaling, Legacy autoscaling)
    - Workers : Allows you to define the number of workers if the mode is fixed or the minimum and maximum number of workers if the mode is not fixed

Useful information after creating the DLT pipeline :
- Execution Mode :
    - Development : In this mode, the cluster used by the DLT pipeline will stop only after 2h (by default) to avoid the restart time of a cluster and there is no automatic restart management.
        - It is possible to modify the delay time before cluster shutdown by configuring the parameter `pipelines.clusterShutdown.delay` with the desired value (for example "300s" for 5 minutes)
        - Example : `{"configuration": {"pipelines.clusterShutdown.delay": "300s"}})`
    - Production : In this mode, the cluster stops automatically after the execution of the DLT pipeline and it restarts automatically in the event of a technical issue (memory leak, authorizations, startup, etc.)
- Schedule :
    - Allows you to define a trigger constraint for the DLT pipeline execution
- Execution :
    - Start : To run the DLT pipeline considering only new available data
    - Start with Full refresh all : To run the DLT pipeline considering all available data
    - Select tables for refresh : To define the objects that we want to update during the DLT pipeline execution (partial execution)

Warning : 
- It is not possible to run the defined script or notebook locally or with a Databricks cluster.
    - You will get the following message : `This Delta Live Tables query is syntactically valid, but you must create a pipeline in order to define and populate your table.`
- A DLT pipeline corresponds to one script or one notebook only, therefore if you want to run several scripts or notebooks, you must use the "Job" feature (from the "Workflows" feature) to orchestrate the execution of several DLT pipelines.
    - Execute the "Job" will run the DLT pipelines with the defined constraints and orchestration
- It is only possible to access an object with the syntax of the DLT framework within the same DLT pipeline, if you need to read an object outside the DLT pipeline then it will be accessed as an external object and he will not be tracked in the graph and in the event logs of the DLT pipeline.
    - Syntax for accessing an object within the same DLT pipeline : `dlt.read(<object>)` or `dlt.read_stream(<object>)` with Python and `LIVE.<object>` or `STREAM(LIVE.<object>)` with SQL.


Regarding the data management associated with a DLT pipeline (maintenance) :
- The DLT framework performs automatic maintenance of each object (Delta table) updated within 24h after the last execution of a DLT pipeline.
    - By default, the system performs a full OPTIMIZE operation, followed by a VACUUM operation
    - If you do not want a Delta table to be automated by default, you must use the `pipelines.autoOptimize.managed = false` property when defining the object (TBLPROPERTIES).


Restrictions : 
- It is not possible to mix the use of Hive Metastore with the Unity Catalog solution or to switch between the two metastores for the target of the DLT pipeline after its creation.
- All tables created and updated by a DLT pipeline are Delta tables
- Objects managed by the DLT framework can only be defined once, that means they can only be the target in one DLT pipeline only (can't define the same object in the same target schema in two different DLT pipelines )
- A Databricks Workspace is limited to 100 DLT pipeline updates



# How it works with Unity Catalog

## Overview

In order to use Unity Catalog, when creating the DLT pipeline you must fill the destination with the Unity Catalog information (with target catalog and target schema).

It is not possible to use the recommended "three namespaces" notation `catalog.schema.object` to access an object managed by the DLT framework.
Objects are defined by name only, and it is the definition of the target catalog and target schema in the DLT pipeline that defines where the objects will be created.
Note: read access to objects not managed by the DLT framework is possible using classic spark syntax (`spark.table("catalog.schema.object")`).

In order to use Unity Catalog with Delta Live Tables, you must have the following rights as the owner of the DLT pipeline : 
- You must have "USE CATALOG" rights on the target catalog
- You must have "USE SCHEMA" rights on the target schema
- You must have "CREATE MATERIALIZED VIEW" rights on the target schema to be able to create "Materialized View" objects
- You must have "CREATE TABLE" rights on the target schema to be able to create "Streaming table" objects


Advice on file ingestion :
- When retrieving information (metadata) about ingested files, it is not possible to use the `input_file_name` function, which is not supported by Unity Catalog.
    - Use the `_metadata` column to retrieve the necessary information, such as the file name `_metadata.file_name`
    - You will find the list of information available in the `_metadata` column on the [official documentation](https://docs.databricks.com/ingestion/file-metadata-column.html)


## Regarding object management by the DLT framework (during DLT pipeline execution)

In reality, when you define an object with the DLT framework, an object will be created in the target schema indicated in the DLT pipeline (with a specific type that will not be a Delta table directly) and a Delta table will be created in an internal schema (managed by the system and not accessible by default to users) to manage the storage of data from the object defined with the DLT framework.
The internal schema is located in the catalog named `__databricks__internal` and owned by `System`.
The default schema name is : `__dlt_materialization_schema_<pipeline_id>`.
In this schema, you'll find all the Delta tables whose storage is managed directly by the DLT framework (the owner of the Delta tables is the owner of the DLT pipeline), as well as the Delta table `__event_log`, which will contain all the event logs of DLT pipeline executions.
[![schema_03](/blog/web/20230612_databricks_unity_catalog_deltalivetables_03.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_03.png)

Example of the definition of a "Streaming Table" object in the "ctg.sch" schema in a DLT pipeline whose identifier is "0000". The data management will be done as follows:
1. Creating a Delta table (with a unique identifier) ​​in an internal system-maintained schema named `__databricks__internal.__dlt_materialization_schema_0000`
    1. With subdirectory (at the Delta data storage level) `_dlt_metadata` containing a `checkpoints` directory to manage the information needed to manage streaming data ingestion
2. Creation of an object of type "STREAMING_TABLE" in the schema "ctg.sch" which refers to the Delta table created in the internal schema `__databricks__internal.__dlt_materialization_schema_0000`
    1. This allows accessing data from the internal Delta table with some constraints

Example of the definition of a "Materialized View" object in the "ctg.sch" schema in a DLT pipeline whose identifier is "0000". The data management will be done as follows:
1. Creating a Delta table (with a unique identifier) ​​in an internal system-maintained schema named `__databricks__internal.__dlt_materialization_schema_0000`
    1. With subdirectory (at the Delta data storage level) `_dlt_metadata` containing a `_enzyme_log` directory to manage the information needed by the Enzyme Project to manage the refresh of object data
2. Creation of an object of type "MATERIALIZED_VIEW" in the schema "ctg.sch" which refers to the Delta table created in the internal schema `__databricks__internal.__dlt_materialization_schema_0000`
    1. This allows accessing data from the internal Delta table with some constraints

Note: The "View" object does not require object creation in the target or internal schema.

## Restrictions

It is also possible to take advantage of the Data Lineage functionality of the Unity Catalog solution with the following limitations:
- There is no trace of source objects that are not defined in a DLT pipeline, so the lineage view is incomplete when the DLT pipeline does not contain all elements.
    - For example, if a referential data source is used as the source of a "Materialized View" object, the lineage will not have the information about this source.
- Lineage doesn't take temporary views into account, and this can prevent a link being made between the view sources and the target object.

Limitations (non-exhaustive) when using Unity Catalog with Delta Live Table :
- It is not possible to change the owner of a pipeline using Unity Catalog.
- Objects are managed/stored only in Unity Catalog's default Metastore storage, it is not possible to define another path (managed or external).
- Delta Sharing cannot be used with objects managed by the DLT framework.
- DLT pipeline event logs can only be read by the DLT pipeline (by default).
- Access to data on certain objects can only be made using a SQL Warehouse (Pro or Serverless) or a Cluster (Shared).


# Set-up the environment

## Context

Prerequisite :
- Creation of the `grp_demo` group
- Create `john.do.dbx@gmail.com` user and add user to `grp_demo` group.
- Create a SQL Warehouse and give use rights to the `grp_demo` group
- Metastore named `metastore-sandbox` with Storage Credential named `sc-metastore-sandbox` to store default data in AWS S3 resource named `s3-dbx-metastore-uc`.
- Add the right to create a catalog on the Metastore Unity Catalog and the right to access files
```sql
GRANT CREATE CATALOG on METASTORE to grp_demo;
GRANT SELECT ON ANY FILE TO grp_demo;
```

To make the examples easier to understand, we'll use the following list of environment variables:
```bash
# Create an alias to use the tool curl with .netrc file
alias dbx-api='curl --netrc-file ~/.databricks/.netrc'

# Create an environment variable with Databricks API URL
export DBX_API_URL="<Workspace Databricks AWS URL>"

# Init local variables
export LOC_PATH_SCRIPT="<Local Path for the folder with the Python script>"
export LOC_PATH_DATA="<Local Path for the folder with the CSV files>"
# Name for DLT Script (Python)
export LOC_SCRIPT_DLT="dlt_pipeline.py"
# List CSV files for the first execution
export LOC_SCRIPT_DATA_1=(ref_products_20230501.csv ref_clients_20230501.csv fct_transactions_20230501.csv)
# List CSV viles for the second execution
export LOC_SCRIPT_DATA_2=(ref_clients_20230601.csv fct_transactions_20230601.csv)

# Init Databricks variables (Workspace)
# Path to store the CSV files
export DBX_PATH_DATA="dbfs:/mnt/dlt_demo/data"
# Name for the DLT pipeline
export DBX_DLT_PIPELINE_NAME="DLT_pipeline_demo"
# DLT pipeline Target Catalog
export DBX_UC_CATALOG="CTG_DLT_DEMO"
# DLT pipeline Target Schema
export DBX_UC_SCHEMA="SCH_DLT_DEMO"
# Path to store the DLT Script (Python)
export DBX_USER_NBK_PATH="/Users/john.do.dbx@gmail.com"

# Init Pipeline variable
export DBX_DLT_PIPELINE_ID=""
```


## Schématisation de l'environnement

Diagram of the DLT pipeline we are going to set up :
[![schema_04](/blog/web/20230612_databricks_unity_catalog_deltalivetables_04.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_04.png)

Details of the DLT pipeline:
- Stream product referential data from CSV file (ref_products_YYYMMDD.csv) to a "Streaming Table" object named "REF_PRODUCTS_RAW".
- Stream data from the "REF_PRODUCTS_RAW" object to a "Streaming Table" object named "REF_PRODUCTS".
- Stream customer referential data from CSV file (ref_clients_YYYMMDD.csv) to a "Streaming Table" object named "REF_CLIENTS_RAW" (with Change Data Feed enabled)
- Stream data from the "REF_PRODUCTS_RAW" object to a "Streaming Table" object using SCD Type 2 functionality and named "REF_CLIENTS". 
    - Using a view named "REF_CLIENTS_CAST" to transform upstream data
- Stream transaction data from CSV file (fct_transactions_YYYMMDD.csv) to a "Streaming Table" object named "FCT_TRX_RAW".
- Ingest data in a "Streaming Table" object named "FCT_TRX" from the "FCT_TRX_RAW" object, applying a constraint on the "quantity" information which must not be equal to 0.
- Feed a "Materialized View" object named "FCT_TRX_AGG_MONTH" from the "FCT_TRX", "REF_CLIENTS" and "REF_PRODUCTS" objects to aggregate sales by month, customer and product.

## Setting up a dataset

The dataset will be build of two batches in order to run two executions and put data refreshment into practice.

Dataset for batch n°1 (first execution of the DLT pipeline) :  
Content of the file `ref_clients_20230501.csv` : 
```text
id,lib,age,contact,phone,is_member,last_maj
1,Maxence,23,max1235@ter.tt,+33232301123,No,2023-01-01 11:01:02
2,Bruce,26,br_br@ter.tt,+33230033155,Yes,2023-01-01 13:01:00
3,Charline,40,ccccharline@ter.tt,+33891234192,Yes,2023-03-02 09:00:00
```

Content of the file `ref_products_20230501.csv` :
```text
id,lib,brand,os,last_maj
1,Pixel 7 Pro,Google,Android,2023-01-01 09:00:00
2,Iphone 14,Apple,IOS,2023-01-01 09:00:00
3,Galaxy S23,Samsung,Android,2023-01-01 09:00:00
```

Content of the file `fct_transactions_20230501.csv` :
```text
id_trx,ts_trx,id_product,id_shop,id_client,quantity
1,2023-04-01 09:00:00,1,2,1,1
2,2023-04-01 11:00:00,1,1,1,3
```


Dataset for batch n°2 (second execution of the DLT pipeline) :  
Content of the file `ref_clients_20230601.csv` : 
```text
id,lib,age,contact,phone,is_member,last_maj
2,Bruce,26,br_br@ter.tt,+33990033100,Yes,2023-04-01 12:00:00
```

Content of the file `fct_transactions_20230601.csv` :
```text
id_trx,ts_trx,id_product,id_shop,id_client,quantity
3,2023-04-03 14:00:00,1,2,1,1
4,2023-04-05 08:00:00,3,1,2,9
5,2023-04-06 10:00:00,1,2,1,3
6,2023-04-06 12:00:00,2,2,1,1
7,2023-01-01 13:00:00,1,2,1,0
8,2023-04-10 18:30:00,2,1,2,11
9,2023-04-10 18:30:00,3,1,2,2
```


## Setting up the objects on the Unity Catalog Metastore 

Steps for creating the necessary objects : 
1. Create a catalog named `ctg_dlt_demo`.
2. Create a schema named `sch_dlt_demo`.

```sql
-- 1. Create Catalog
CREATE CATALOG IF NOT EXISTS ctg_dlt_demo
COMMENT 'Catalog to store the dlt demo objects';

-- 2. Create Schema
CREATE SCHEMA IF NOT EXISTS ctg_dlt_demo.sch_dlt_demo
COMMENT 'Schema to store the dlt demo objects';
```


## Creation of the python script containing the objects definition for the DLT framework

The script is named `dlt_pipeline.py` and will be copied to the Databricks Workspace (it will be used by the DLT pipeline).

The Python script contains the following code :
```python
"""Pipeline DLT demo"""
import dlt
from pyspark.sql.functions import col, current_timestamp, expr, sum

# Folder to store data for the demo
PATH_DATA = "dbfs:/mnt/dlt_demo/data"


###################
## Products Data ##
###################

# Create the streaming table named REF_PRODUCTS_RAW
@dlt.table(
  name="REF_PRODUCTS_RAW",
  comment="Raw Products Referential Data",
  table_properties={"quality" : "bronze"},
  temporary=False)
def get_products_raw():
    return spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "csv") \
                .option("delimiter",",") \
                .option("header","true") \
                .load(PATH_DATA+"/ref_products_*.csv") \
                .select("*"
                        ,col("_metadata.file_name").alias("source_file")
                        ,current_timestamp().alias("processing_time"))

# Create the streaming table named REF_PRODUCTS
@dlt.table(
  name="REF_PRODUCTS",
  comment="Products Referential Data",
  table_properties={"quality" : "silver"},
  temporary=False)
def get_products():
   return dlt.read_stream("REF_PRODUCTS_RAW") \
            .where("_rescued_data is null") \
            .select(col("id").cast("INT")
                    ,col("lib")
                    ,col("brand")
                    ,col("os")
                    ,col("last_maj").cast("TIMESTAMP")
                    ,col("source_file")
                    ,col("processing_time"))


###################
## Clients Data ##
##################

# Create the streaming table named REF_CLIENTS_RAW
@dlt.table(
  name="REF_CLIENTS_RAW",
  comment="Raw Clients Referential Data",
  table_properties={"quality" : "bronze", "delta.enableChangeDataFeed" : "true"},
  temporary=False)
def get_products_raw():
    return spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "csv") \
                .option("delimiter",",") \
                .option("header","true") \
                .load(PATH_DATA+"/ref_clients_*.csv") \
                .select("*"
                        ,col("_metadata.file_name").alias("source_file")
                        ,current_timestamp().alias("processing_time"))

# Create the temporary view named REF_CLIENTS_RAW_CAST
@dlt.view(
      name="REF_CLIENTS_RAW_CAST"
      ,comment="Temp view for Clients Raw Casting")
def view_clients_cast():
  return dlt.read_stream("REF_CLIENTS_RAW") \
            .select(expr("cast(id as INT) as id")
                    ,col("lib")
                    ,expr("cast(age as INT) as age")
                    ,col("contact")
                    ,col("phone")
                    ,expr("(is_member = 'Yes') as is_member")
                    ,expr("cast(last_maj as timestamp) as last_maj")
                    ,col("source_file")
                    ,col("processing_time"))

# Create the streaming table named REF_CLIENTS (and using SCD Type 2 management)
dlt.create_streaming_table(
  name = "REF_CLIENTS",
  comment = "Clients Referential Data (SCD2)"
)

# Apply modification (for SCD Type 2 management) based on the temporary view
dlt.apply_changes(target = "REF_CLIENTS",
                source = "REF_CLIENTS_RAW_CAST",
                keys = ["id"],
                sequence_by = col("last_maj"),
                stored_as_scd_type = 2)


#######################
## Transactions Data ##
#######################

# Create the streaming table named FCT_TRX_RAW
@dlt.table(
    name="FCT_TRX_RAW",
    comment="Raw Transactions Fact Data",
    table_properties={"quality" : "bronze"},
    partition_cols=["dt_tech"],
    temporary=False)
def get_transactions_raw():
    return spark.readStream.format("cloudFiles") \
                .option("cloudFiles.format", "csv") \
                .option("delimiter",",") \
                .option("header","true") \
                .load(PATH_DATA+"/fct_transactions_*.csv") \
                .select("*"
                        ,col("_metadata.file_name").alias("source_file")
                        ,current_timestamp().alias("processing_time")
                        ,expr("current_date() as dt_tech"))\


# Create the streaming table named FCT_TRX (with expectation)
@dlt.table(
    name="FCT_TRX",
    comment="Transactions Fact Data",
    table_properties={"quality" : "silver"},
    partition_cols=["dt_trx"],
    temporary=False)
@dlt.expect("valide quantity","quantity <> 0")
def get_transactions():
    return dlt.read_stream("FCT_TRX_RAW") \
                .where("_rescued_data IS NULL") \
                .select(expr("cast(id_trx as INT) as id_trx")
                        ,expr("cast(ts_trx as timestamp) as ts_trx")
                        ,expr("cast(id_product as INT) as id_product")
                        ,expr("cast(id_shop as INT) as id_shop")
                        ,expr("cast(id_client as INT) as id_client")
                        ,expr("cast(quantity as DOUBLE) as quantity")
                        ,col("source_file")
                        ,col("processing_time")
                        ,col("dt_tech")) \
                .withColumn("_invalid_data",expr("not(coalesce(quantity,0) <> 0)")) \
                .withColumn("dt_trx",expr("cast(ts_trx  as date)"))


# Create the Materialized View named FCT_TRX_AGG_MONTH
@dlt.table(
    name="FCT_TRX_AGG_MONTH",
    comment="Transactions Fact Data Aggregate Month",
    table_properties={"quality" : "gold"},
    partition_cols=["dt_month"],
    temporary=False)
def get_transactions_agg_month():
    data_fct = dlt.read("FCT_TRX").where("not(_invalid_data)")
    data_ref_products = dlt.read("REF_PRODUCTS")
    data_ref_clients = dlt.read("REF_CLIENTS").where("__END_AT IS NULL")
    return data_fct.alias("fct").join(data_ref_products.alias("rp"), data_fct.id_product == data_ref_products.id, how="inner") \
                .join(data_ref_clients.alias("rc"),data_fct.id_client == data_ref_clients.id, how="inner") \
                .withColumn("dt_month",expr("cast(substring(cast(fct.dt_trx as STRING),0,7)||'-01' as date)")) \
                .groupBy("dt_month","rc.lib","rc.contact","rp.brand") \
                .agg(sum("fct.quantity").alias("sum_quantity")) \
                .select(col("dt_month")
                        ,col("rc.lib")
                        ,col("rc.contact")
                        ,col("rp.brand")
                        ,col("sum_quantity").alias("quantity")
                        ,expr("current_timestamp() as ts_tech"))
```


# Creating a DLT Pipeline

To create the DLT pipeline, we will follow these steps:
1. Create a directory in the DBFS directory to store CSV files
2. Copy the Python script to the Databricks Workspace
3. Create the DLT pipeline (taking as source the Python script copied to the Workspace Databricks)
4. Retrieve the DLT pipeline identifier in an environment variable named "DBX_DLT_PIPELINE_NAME"


Using Databricks REST APIs : 
```bash
# 1. Create the directory to store CSV files (on the DBFS)
dbx-api -X POST ${DBX_API_URL}/api/2.0/dbfs/mkdirs -H 'Content-Type: application/json' -d "{
    \"path\": \"${DBX_PATH_DATA}\"
}"

# 2. Copy the Python script into the Workspace
dbx-api -X POST ${DBX_API_URL}/api/2.0/workspace/import -H 'Content-Type: application/json' -d "{
    \"format\": \"SOURCE\",
    \"path\": \"${DBX_USER_NBK_PATH}/${LOC_SCRIPT_DLT}\",
    \"content\" : \"$(base64 -i ${LOC_PATH_SCRIPT}/${LOC_SCRIPT_DLT})\",
    \"language\": \"PYTHON\",
    \"overwrite\": \"true\"
}"

# 3. Create the DLT Pipeline based on the Python script
dbx-api -X POST ${DBX_API_URL}/api/2.0/pipelines -H 'Content-Type: application/json' -d "
{
    \"continuous\": false,
    \"name\": \"${DBX_DLT_PIPELINE_NAME}\",
    \"channel\": \"PREVIEW\",
    \"catalog\": \"${DBX_UC_CATALOG}\",
    \"target\": \"${DBX_UC_SCHEMA}\",
    \"development\": true,
    \"photon\": false,
    \"edition\": \"ADVANCED\",
    \"allow_duplicate_names\": \"false\",
    \"dry_run\": false,
    \"configuration\": {
      \"pipelines.clusterShutdown.delay\": \"600s\"
    },
    \"clusters\": [
      {
        \"label\": \"default\",
        \"num_workers\": 1
      }
    ],
    \"libraries\": [
      {
        \"notebook\": {
          \"path\": \"${DBX_USER_NBK_PATH}/${LOC_SCRIPT_DLT}\"
        }
      }
    ]
}"

# 4. Retrieve the Pipeline ID
export DBX_DLT_PIPELINE_ID=`dbx-api -X GET ${DBX_API_URL}/api/2.0/pipelines | jq -r '.statuses[]|select(.name==$ENV.DBX_DLT_PIPELINE_NAME)|.pipeline_id'`
```


# Running and Viewing a DLT Pipeline

## Running a DLT Pipeline

In order to be able to run the first execution of the DLT pipeline, the following actions must be done :
1. Copy the CSV data from batch n°1 to the DBFS directory (on the Databricks Workspace)
2. Run the DLT pipeline
3. Retrieve DLT pipeline status

Using Databricks REST APIs : 
```bash
# 1. Upload CSV Files (for execution)
for file in ${LOC_SCRIPT_DATA_1}; do 
    dbx-api -X POST ${DBX_API_URL}/api/2.0/dbfs/put -H 'Content-Type: application/json' -d "{
        \"path\": \"${DBX_PATH_DATA}/${file}\",
        \"contents\": \"$(base64 -i ${LOC_PATH_DATA}/${file})\",
        \"overwrite\": \"true\"
        }"
done

# 2. Execute the DLT Pipeline
dbx-api -X POST ${DBX_API_URL}/api/2.0/pipelines/${DBX_DLT_PIPELINE_ID}/updates -H 'Content-Type: application/json' -d "
{
    \"full_refresh\": false,
    \"cause\": \"USER_ACTION\"
}"

# 3. Retrieve the status of the DLT Pipeline execution
dbx-api -X GET ${DBX_API_URL}/api/2.0/pipelines/${DBX_DLT_PIPELINE_ID}/events | jq '.events[0]|.details'
```


In order to be able to run the second execution of the DLT pipeline, the following actions must be done :
1. Copy the CSV data from batch n°2 to the DBFS directory (on the Databricks Workspace)
2. Run the DLT pipeline
3. Retrieve DLT pipeline status

Using Databricks REST APIs :
```bash
# 1. Upload CSV Files (for execution)
for file in ${LOC_SCRIPT_DATA_2}; do 
    dbx-api -X POST ${DBX_API_URL}/api/2.0/dbfs/put -H 'Content-Type: application/json' -d "{
        \"path\": \"${DBX_PATH_DATA}/${file}\",
        \"contents\": \"$(base64 -i ${LOC_PATH_DATA}/${file})\",
        \"overwrite\": \"true\"
        }"
done

# 2. Execute the DLT Pipeline
dbx-api -X POST ${DBX_API_URL}/api/2.0/pipelines/${DBX_DLT_PIPELINE_ID}/updates -H 'Content-Type: application/json' -d "
{
    \"full_refresh\": false,
    \"cause\": \"USER_ACTION\"
}"

# 3. Retrieve the status of the DLT Pipeline execution
dbx-api -X GET ${DBX_API_URL}/api/2.0/pipelines/${DBX_DLT_PIPELINE_ID}/events | jq '.events[0]|.details'
```


## DLT Pipeline Visualization

Result after the first execution of the DLT pipeline :
[![schema_05](/blog/web/20230612_databricks_unity_catalog_deltalivetables_05.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_05.png)

Detail of the "FCT_TRX" object (with the constraint/expectation on the "quantity" information) :
[![schema_06](/blog/web/20230612_databricks_unity_catalog_deltalivetables_06.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_06.png)

Detail of the "REF_CLIENTS" object (feeded by the SCD type 2 process) :
[![schema_07](/blog/web/20230612_databricks_unity_catalog_deltalivetables_07.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_07.png)


Result after the seoncd execution of the DLT pipeline ::
[![schema_08](/blog/web/20230612_databricks_unity_catalog_deltalivetables_08.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_08.png)


Detail of the "FCT_TRX" object (with the constraint/expectation on the "quantity" information) :
[![schema_09](/blog/web/20230612_databricks_unity_catalog_deltalivetables_08.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_09.png)

Detail of the "REF_CLIENTS" object (feeded by the SCD type 2 process) :
[![schema_10](/blog/web/20230612_databricks_unity_catalog_deltalivetables_10.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_10.png)

Example of the tab containing the event logs of the second execution of the DLT pipeline :
[![schema_11](/blog/web/20230612_databricks_unity_catalog_deltalivetables_11.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_11.png)


Oberservation :
- The DLT pipeline graph makes it very easy to visualize the lineage of the data as well as the number of records processed (or deleted) during each step (object)
- When we click on an object, we can have metrics on the object (number of records written and deleted, number of records violating a constraint, schema of the object, date and time of execution, duration, . ..)
- At the bottom of the screen, we have access to all the event logs of the execution of the DLT pipeline and it is possible to filter them on the options "All", "Info", "Warning" and " Error" or on the description of the events.
- It is possible to choose the execution you want to view by selecting it from the drop-down list (by execution timestamp and containing the last execution status).


## Visualization with Unity Catalog

Visualization of the objects in the "sch_dlt_demo" schema in the "ctg_dlt_demo" catalog:
[![schema_12](/blog/web/20230612_databricks_unity_catalog_deltalivetables_12.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_12.png)

Viewing details for the "FCT_TRX" object which is a "Streaming Table" object :
[![schema_13](/blog/web/20230612_databricks_unity_catalog_deltalivetables_13.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_13.png)

Viewing details for the "FCT_TRX_AGG_MONTH" object which is a "Materialized View" object :
[![schema_14](/blog/web/20230612_databricks_unity_catalog_deltalivetables_14.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_14.png)

Observation : 
- We can see that the "Storage Location" information does not exist because they are not actually Delta tables but they can be seen as logical objects.


Regarding Data Lineage : 
Visualization of the lineage starting from the "REF_CLIENTS" object :
[![schema_15](/blog/web/20230612_databricks_unity_catalog_deltalivetables_15.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_15.png)
Observation : 
- We can observe that there is information for the objects defined in the DLT pipeline except for the data source of the object "REF_CLIENTS" which is named "REF_CLIENTS_RAW"
- We can observe that there is no information about data sources (CSV files)

Visualization of the lineage starting from the "REF_CLIENTS_RAW" object :
[![schema_16](/blog/web/20230612_databricks_unity_catalog_deltalivetables_16.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_16.png)
Observation : 
- We can observe that the "REF_CLIENTS_RAW" object has no lineage information while it is the source of the "REF_CLIENTS" object's in the DLT pipeline



Regarding internal objects :
Warning: To be able to view them, you must have the following rights
```sql
GRANT USE_CATALOG ON CATALOG __databricks_internal TO <user or group>;
GRANT USE_SCHEMA ON SCHEMA __databricks_internal.__dlt_materialization_schema_<pipeline_id> TO <user or group>;
```

Visualizing the `__databricks_internal` internal catalog with Data Explorer
[![schema_17](/blog/web/20230612_databricks_unity_catalog_deltalivetables_17.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_17.png)

Visualization of details for the "FCT_TRX" internal object (which contains the data of a "Streaming Table" object) :
[![schema_18](/blog/web/20230612_databricks_unity_catalog_deltalivetables_18.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_18.png)

Visualization of details for the "FCT_TRX_AGG_MONTH" internal object (which contains the data of a "Materialized View" object) :
[![schema_19](/blog/web/20230612_databricks_unity_catalog_deltalivetables_19.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_19.png)

Observation : 
- We can observe that these are Delta tables


# Monitoring a DLT pipeline

Monitoring a DLT pipeline relies on event logs related to the execution of the DLT pipeline, including audit, data quality, and lineage logs.
This enables analysis of executions and status of DLT pipelines.

The `__event_log` internal object is a Delta table which contains a subdirectory named `_dlt_metadata` and containing an `_autoloader` directory with information allowing to manage the loading of data by the system with Auto Loader.

There are four methods of accessing event logs regarding the DLT pipelines execution :
1. The 1st method is to use the Databricks Workspace user interface
    1. Click on the "Workflows" option in the side menu
    2. Click on the "Delta Live Tables" tab
    3. Select the desired DLT pipeline
    4. Select the desired DLT pipeline run (sorted by descending start timestamp)
    5. All event logs can be found in the tab at the bottom of the interface
2. The 2nd method is to use the [Databricks REST API](https://docs.databricks.com/api/workspace/pipelines/listpipelineevents)
3. The 3rd method is to use the `event_log(<pipeline_id)` function (table valued function), this is the method recommended by Databricks
4. The 4th method is to access the Delta table `__event_log` located in the internal schema linked to the DLT pipeline
    1. This method requires having the "USE" right on the internal schema and on the corresponding internal catalog
```sql
GRANT USE_CATALOG ON CATALOG __databricks_internal TO <user or group>;
GRANT USE_SCHEMA ON SCHEMA __databricks_internal.__dlt_materialization_schema_<pipeline_id> TO <user or group>;
```

DLT pipeline event log storage is physically separated for each DLT pipeline and there is no default view that provides an aggregated view of event logs.
You will find detail of the event logs schema in the [official documentation](https://docs.databricks.com/delta-live-tables/observability.html#event-log-schema)

The types of existing events are as follows (not exhaustive):
- user_action: Information about user actions (creation of a DLT pipeline, execution of a DLT pipeline, stop of a DLT pipeline)
- create_update: Information about the DLT pipeline execution request (origin of the request)
- update_progress: Information about the execution steps of the DLT pipeline (WAITING_FOR_RESOURCES, INITIALIZING, SETTING_UP_TABLES, RUNNING, COMPLETED, FAILED)
- flow_definition: Information about the definition of objects (Type of update (INCREMENTAL, CHANGE, COMPLETE), object schema, ...)
- dataset_definition: Information about the definition of objects (schema, storage, type, ...)
- graph_created: Information about the creation of the graph during the execution of the DLT pipeline
- planning_information: Information about the refresh schedule for "Materialized View" objects
- flow_progress: Information about the execution steps of all the elements defined in the DLT pipeline (QUEUED, STARTING, RUNNING, COMPLETED, FAILED)
- cluster_resources: Information about DLT pipeline cluster resource management
- maintenance_progress: Information about the maintenance operations on the data within 24 hours after the last execution of the DLT pipeline


Warning :
- Metrics are not captured for "Streaming Table" objects feeded with the Type 2 Slow Changing Dimension (SCD) process managed by the DLT framework
    - Nevertheless, it is possible to have metrics by accessing the history of the internal Delta table directly
- When there are records that do not respect the constraints on an object, we have access to the metrics on the number of records but not the details of the records concerned.

It is very easy to set up event logs based dashboards or exports to centralize information from the Databricks REST API, SQL Warehouse or by adding processing (in a Job) after the execution of each DLT pipeline to retrieve the necessary information.

We will use the 3rd method to give examples of queries to analyze the DLT pipeline.

1/ Example query to retrieve all information for the last run of the DLT pipeline :
```sql
with updid (
  select row_number() over (order by ts_start_pipeline desc) as num_exec_desc
        ,update_id
        ,ts_start_pipeline
  from (
    select origin.update_id as update_id,min(timestamp) as ts_start_pipeline
    from event_log('a64f295a-1655-43ca-9543-f1dd1e73009c')
    where origin.update_id is not null
    group by origin.update_id
  )
)
select l.* 
from event_log('a64f295a-1655-43ca-9543-f1dd1e73009c') l
where l.origin.update_id = (select update_id from updid where num_exec_desc = 1)
order by timestamp desc
```

Result :
[![schema_20](/blog/web/20230612_databricks_unity_catalog_deltalivetables_20.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_20.png)

2/ Example of a query to retrieve the number of lines written for each object during the two executions of the DLT pipeline :
```sql
with updid (
  select row_number() over (order by ts_start_pipeline desc) as num_exec_desc
        ,update_id
        ,ts_start_pipeline
  from (
    select origin.update_id as update_id,min(timestamp) as ts_start_pipeline
    from event_log('a64f295a-1655-43ca-9543-f1dd1e73009c')
    where origin.update_id is not null
    group by origin.update_id
  )
)
select l.origin.flow_name
  ,u.ts_start_pipeline
  ,sum(l.details:['flow_progress']['metrics']['num_output_rows']) as num_output_rows
  ,sum(l.details:['flow_progress']['metrics']['num_upserted_rows']) as num_upserted_rows
  ,sum(l.details:['flow_progress']['metrics']['num_deleted_rows']) as num_deleted_rows
from event_log('a64f295a-1655-43ca-9543-f1dd1e73009c') l
inner join updid u
on (l.origin.update_id = u.update_id)
and event_type = 'flow_progress' 
and details:['flow_progress']['metrics'] is not null
group by l.origin.flow_name
  ,u.ts_start_pipeline
order by l.origin.flow_name
  ,u.ts_start_pipeline
```
Comments :
- When the object is a "Streaming Table", the metrics are captured with the event that has the "RUNNING" status
    - Special case for the "Streaming Table" object using the SCD Type 2 process whose metrics are not captured
- When the object is a "Materialized View", the metrics are captured with the event which has the status "COMPLETED"
- When the object is a "View", no metrics are captured

Result : 
[![schema_21](/blog/web/20230612_databricks_unity_catalog_deltalivetables_21.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_21.png)

Comments :
- We can observe that the "REF_PRODUCTS" and "REF_PRODUCTS_RAW" objects had no new data on the second run
- We can observe that the "REF_CLIENT" object has no information while the "REF_CLIENT_RAW" source object has retrieved only the new data during each execution of the DLT pipeline


3/ Example query to retrieve the number of records violating the constraints for the "FCT_TRX" object during each execution of the DLT pipeline:
```sql
with updid (
  select row_number() over (order by ts_start_pipeline desc) as num_exec_desc
        ,update_id
        ,ts_start_pipeline
  from (
    select origin.update_id as update_id,min(timestamp) as ts_start_pipeline
    from event_log('a64f295a-1655-43ca-9543-f1dd1e73009c')
    where origin.update_id is not null
    group by origin.update_id
  )
)
select flow_name
  ,metrics.name
  ,ts_start_pipeline
  ,sum(metrics.passed_records) as passed_records
  ,sum(metrics.failed_records) as failed_records
from (
  select l.origin.flow_name as flow_name
    ,u.ts_start_pipeline as ts_start_pipeline
    ,explode(from_json(l.details:['flow_progress']['data_quality']['expectations'][*], 'array<struct<name string, passed_records int, failed_records int >>')) as metrics
  from event_log('a64f295a-1655-43ca-9543-f1dd1e73009c') l
  inner join updid u
  on (l.origin.update_id = u.update_id)
  and event_type = 'flow_progress' 
  and origin.flow_name = "fct_trx"
  and details:['flow_progress']['data_quality'] is not null
) wrk
group by flow_name
  ,metrics.name
  ,ts_start_pipeline
order by flow_name
  ,metrics.name
  ,ts_start_pipeline
```

Result :
[![schema_22](/blog/web/20230612_databricks_unity_catalog_deltalivetables_22.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_22.png)


4/ Example query to retrieve the start and end timestamp of each run of the DLT pipeline :
```sql
with updid (
  select row_number() over (order by ts_start_pipeline desc) as num_exec_desc
        ,update_id
        ,ts_start_pipeline
  from (
    select origin.update_id as update_id,min(timestamp) as ts_start_pipeline
    from event_log('a64f295a-1655-43ca-9543-f1dd1e73009c')
    where origin.update_id is not null
    group by origin.update_id
  )
)
select update_id
,start_time
,end_time
,end_time - start_time as duration
,last_state
from (
  select l.origin.update_id as update_id
    ,min(l.timestamp) over (partition by l.origin.update_id) as start_time
    ,max(l.timestamp) over (partition by l.origin.update_id) as end_time
    ,row_number() over (partition by l.origin.update_id order by timestamp desc) as num
    ,l.details:['update_progress']['state'] as last_state
  from event_log('a64f295a-1655-43ca-9543-f1dd1e73009c') l
  inner join updid u
  on (l.origin.update_id = u.update_id)
  and event_type = 'update_progress' 
) wrk
where num = 1
order by update_id
,start_time asc
```

Result :
[![schema_23](/blog/web/20230612_databricks_unity_catalog_deltalivetables_23.png)](/blog/web/20230612_databricks_unity_catalog_deltalivetables_23.png)




# Clean environment

Delete the DLT pipeline
```bash
dbx-api -X DELETE ${DBX_API_URL}/api/2.0/pipelines/${DBX_DLT_PIPELINE_ID}
```

Delete the Python script from the Databricks Workspace
```bash
dbx-api -X POST ${DBX_API_URL}/api/2.0/workspace/delete -H 'Content-Type: application/json' -d "{
    \"path\": \"${DBX_USER_NBK_PATH}/${LOC_SCRIPT_DLT}\",
    \"recursive\": \"false\"
}"
```

Delete CSV fils from Databricks Workspace DBFS storage
```bash
dbx-api -X POST ${DBX_API_URL}/api/2.0/dbfs/delete -H 'Content-Type: application/json' -d "{
    \"path\": \"${DBX_PATH_DATA}\",
    \"recursive\": \"true\"
}"
```

Delete the "CTG_DLT_DEMO" catalog from the Unity Catalog Metastore :
```sql
-- Delete the Catalog with CASCADE option (to delete all objects)
DROP CATALOG IF EXISTS CTG_DLT_DEMO CASCADE;
```

Warning :
- When we delete the catalog "CTG_DLT_DEMO" and the DLT pipeline, the internal Delta tables are not deleted directly (nor the internal schema).
- It is necessary to wait for the automatic maintenance operation after the deletion of the DLT pipeline so that the elements are deleted.



# Conclusion

The DLT framework makes it possible to be very efficient in the creation and execution of an ETL pipeline and to be able to easily add data quality management (subject to use the Advanced edition).

Being able to view the graph and metrics associated with objects of a specific DLT pipeline run is extremely convenient for quick analysis.
In addition, access to event logs (stored in a Delta table) allows us to retrieve a lot of information and to be able to perform analyzes and dashboards very simply.

During POC or specific ETL ingestion processing, it is very practical to be able to rely on the DLT framework but in the context of a project requiring the management of numerous objects and schemas, we find ourselves much too limited to be able to use the DLT framework.

As it stands (public preview), the limitations are too important for us to recommend using the DLT framework with Unity Catalog for major projects:
- Lineage is incomplete if the object is not managed in the DLT pipeline, or if "View" objects are used.
- Only one target schema can be used for all elements of a DLT pipeline
- Some operations (SCD Type 1 and 2 management) have no metrics captured in event logs
- No aggregation of event logs for all pipelines by default if you wish to perform analyses on all DLT pipelines

In my humble opinion, it's still a bit early to use the DLT framework relying on the Unity Catalog solution to manage all the ETL processes for managing a company's data, but i can't wait to see future improvements made by Databricks to make it a central and powerful tool for managing ETL processes while taking advantage of all the features of the Unity Catalog solution (Data Lineage, Delta Sharing, ...).
