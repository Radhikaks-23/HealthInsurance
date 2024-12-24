# Databricks notebook source
from pyspark.sql import SparkSession , functions as f 

#Read hospital A department file details 
df_hosa = spark.read.parquet("/mnt/datalakehousegen23/bronze/hosa/departments")

#Read hospital B department file details
df_hosb = spark.read.parquet("/mnt/datalakehousegen23/bronze/hosb/departments")

#Union of two department DataFrame 
df_merge = df_hosa.unionByName(df_hosb)

# Create the dept_id column and rename deptid to src_dept_id
df_merge = df_merge.withColumn("src_dept_id", f.col("deptid"))\
                .withColumn("dept_id", f.concat(f.col("DeptID"),f.lit('-'),f.col("datasource"))) \
                .drop("deptid")

display(df_merge)

df_merge.createOrReplaceTempView("departments")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE TABLE IF NOT EXISTS departments (
# MAGIC dept_Id string,
# MAGIC src_dept_Id string,
# MAGIC Name string,
# MAGIC datasource string,
# MAGIC is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC  %sql 
# MAGIC truncate table silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into silver.departments
# MAGIC SELECT 
# MAGIC dept_Id,
# MAGIC src_dept_Id,
# MAGIC Name,
# MAGIC Datasource,
# MAGIC      CASE 
# MAGIC          WHEN src_dept_Id IS NULL OR Name IS NULL THEN TRUE
# MAGIC          ELSE FALSE
# MAGIC      END AS is_quarantined
# MAGIC FROM departments

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.departments