# Real-Time-Streaming-Data-Pipeline-with-AWS-Apache-NiFi-Snowflake

## Project Overview

Developed a real-time customer data pipeline for ingesting, processing, and transforming streaming customer data into a structured, analytics-ready format in Snowflake, leveraging AWS, Apache NiFi, and Snowflake's advanced capabilities.

## Architecture Diagram

![Data Pipeline Architecture Diagram](https://github.com/Ehan-Ghalib/Spotify-Data-Pipeline-with-AWS-Snowflake/blob/53b8593231d3e6c135a393e3a84f5ba447fd06a9/Spotify%20AWS-Snowflake%20Pipeline%20Architecture%20Diagram.png)

## Key Highlights

### Data Generation and Storage

- Created a data simulation script using the Faker library in JupyterLab to generate realistic, large-scale customer data.
- Stored the generated data on an AWS EC2 instance for further processing.

### Data Ingestion:

- Implemented a data transfer pipeline using Apache NiFi installed on the EC2 instance:
  - Transferred the data files from the EC2 instance to an AWS S3 bucket for staging.
  - Ensured robust and scalable file movement through NiFi’s visual workflows.

### Real-Time Streaming to Snowflake

- Leveraged AWS SQS and Snowpipe to stream the customer data from S3 into a raw table in Snowflake.
- Ensured seamless and low-latency data ingestion for real-time use cases.

### Staging and Deduplication

- Processed data from the raw table into a stage table to:
  - Deduplicate records.
  - Prepare the data for dimensional updates (SCD Type 1 and Type 2).

### Dimensional Modeling

- Implemented Slowly Changing Dimensions (SCD) logic:
  - SCD Type 1: Update the current customer details.
  - SCD Type 2: Maintain historical records for analytics.
- Developed watermark columns for efficient incremental data loading.

### Parameter Control for Incremental Loading

- Utilized a Parameter Control table in Snowflake to manage the Last Extracted Time Stamp (LETS):
  - Used LETS to track incremental changes in the raw table.
  - Automated raw table cleanup and stage table truncation after each load.

### Automation via Snowflake Stored Procedures and Tasks

- Designed a comprehensive Snowflake stored procedure to orchestrate the following tasks:
	- Incremental loading from raw to stage to dimensional tables.
	- SCD Type 1 and Type 2 updates.
	- Raw table cleanup based on LETS.
	- Stage table truncation post-processing.
- Scheduled the stored procedure execution using a Snowflake Task, enabling automated and scheduled runs.

## Technical Stack

- Data Simulation: Python, Faker Library
- Data Ingestion and Transfer: Apache NiFi, AWS EC2, AWS S3, AWS SQS, Snowpipe
- Data Warehousing and Transformation: Snowflake (Raw, Stage, and Dimensional tables)
- Orchestration and Scheduling: Snowflake Stored Procedures, Snowflake Tasks
- Deduplication and SCD Processing: Custom SQL logic in Snowflake

## Business Impact

- Enabled real-time ingestion and processing of streaming customer data.
- Ensured historical data retention with SCD Type 2 while maintaining current data accuracy with SCD Type 1.
- Simplified incremental data loading and cleanup with automated parameter control and watermarking.
- Reduced manual intervention and operational overhead with fully automated workflows and task scheduling.
- Provided an analytics-ready customer dimension table for downstream reporting and decision-making.

This project showcases how AWS and Snowflake can work in tandem to build scalable, cost-effective data pipelines that deliver timely insights.

#Snowpipe, #Serverless, #Data Pipeline, #Near-Real Time

