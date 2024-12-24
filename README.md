# HealthInsurance Azure End to End Project 

# Project Overview
This project focuses on building an end-to-end data pipeline to ingest, process, and store healthcare data from various sources, such as EMR (Electronic Medical Records), Claims data from insurance companies, and standardized healthcare codes like NPI (National Provider Identifier) and ICD (International Classification of Diseases). The pipeline uses Azure Data Factory for ingestion, Azure Databricks for data processing, and Azure Data Lake Storage (ADLS Gen2) for storage. The goal is to create a medallion architecture (bronze, silver, and gold layers) to transform the raw data for business reporting and analytics.

# Data Sources:

EMR Data (Azure SQL Database)\
Claims Data (Flat Files)\
NPI Data (National Provider Identifier)_API\
ICD Data (International Classification of Diseases)_API

# Solution Architecture: Medallion Architecture
This project follows the Medallion Architecture (Bronze, Silver, and Gold layers), which is ideal for managing large-scale healthcare data from different sources, ensuring data quality, and making data available for analytics and reporting.

![image](https://github.com/user-attachments/assets/d936c19a-58cf-4efe-b551-cd7e49592b3f)
