# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS NewAudit;
# MAGIC CREATE TABLE IF NOT EXISTS NewAudit.load_logs (
# MAGIC   data_source STRING,
# MAGIC   tablename STRING,
# MAGIC   numberofrowscopied INT,
# MAGIC   watermarkcolumnname STRING,
# MAGIC   loaddate TIMESTAMP
# MAGIC  );

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table  NewAudit.load_logs 

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from NewAudit.load_logs