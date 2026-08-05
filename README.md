# End-to-End Azure Data Engineering Pipeline for AdventureWorks Data

## Overview

This project demonstrates an end-to-end data engineering pipeline built on Microsoft Azure using the AdventureWorks dataset. The pipeline ingests raw CSV files into Azure Data Lake Storage Gen2, performs data transformation and cleansing using Azure Databricks with PySpark, and stores the processed data in Parquet format for efficient analytics.

The project follows the Medallion Architecture by organizing data into Bronze (raw) and Silver (processed) layers.

---

## Architecture

```
AdventureWorks CSV Files
        │
        ▼
Azure Data Factory
(Data Ingestion)
        │
        ▼
Azure Data Lake Storage Gen2
Bronze Layer (Raw CSV)
        │
        ▼
Azure Databricks (PySpark)
Data Cleaning & Transformation
        │
        ▼
Azure Data Lake Storage Gen2
Silver Layer (Parquet Files)
        │
        ▼
Business Analysis using PySpark
```

---

## Workflow

1. Store AdventureWorks CSV files in the Bronze container of Azure Data Lake Storage Gen2.
2. Use Azure Data Factory to orchestrate data ingestion.
3. Authenticate Azure Databricks using a Service Principal.
4. Read raw CSV files from the Bronze layer.
5. Perform data transformations using PySpark.
6. Write transformed datasets as Parquet files into the Silver layer.
7. Execute analytical queries to generate business insights.

---

## Key Transformations

- Extracted **Month** and **Year** from calendar dates.
- Created a **FullName** column for customer records.
- Standardized product SKUs and product names.
- Converted sales timestamps to proper datetime format.
- Generated calculated columns for sales analysis.
- Converted cleaned datasets into optimized Parquet format.

---

## Business Analysis

The project performs several analytical operations, including:

- Daily order analysis
- Average product cost
- Product subcategory distribution
- Customer home ownership analysis
- Customer income analysis
- Territory-wise return analysis

---

## Technologies Used

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Apache Spark (PySpark)
- Python
- Parquet
- Microsoft Entra ID (Service Principal)

---

## Prerequisites

- Azure Subscription
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks Workspace
- Service Principal with Storage permissions

---

## Setup

1. Create an Azure Data Lake Storage Gen2 account.
2. Upload the AdventureWorks dataset to the Bronze container.
3. Configure a Service Principal and grant Storage Blob Data Contributor permissions.
4. Update the notebook with your:
   - Storage Account
   - Tenant ID
   - Client ID
   - Client Secret
5. Run the Azure Data Factory pipeline.
6. Execute the Databricks notebook.
7. Verify the processed Parquet files in the Silver container.

---

## Security Notice

This repository is intended for demonstration purposes.

All Azure credentials, client secrets, tenant IDs, storage account names, and other sensitive information have been removed and replaced with placeholders before publishing.

---

## Future Improvements

- Implement a Gold layer for business-ready datasets.
- Integrate Delta Lake for ACID transactions.
- Schedule automated pipeline execution using ADF triggers.
- Build interactive Power BI dashboards.
- Implement data quality checks and monitoring.

---

## Author

**Ishika Singh**

B.Tech (Computer Science & Engineering - Artificial Intelligence)

Aspiring Data Engineer | SQL | Python | Azure | PySpark
