# Azure Real-Time Data Engineering Pipeline – Uber

## 📌 Project Overview

This project demonstrates a real-time ELT data pipeline using Microsoft Azure services. Streaming events were generated through a sample web application, ingested into Azure Event Hub, transferred to Azure Data Lake Storage using Azure Data Factory, and processed using Azure Databricks Structured Streaming with PySpark.

## 🏗️ Project Workflow

![Project Workflow](Data/Uber_real_time_project_workflow.png)

## 🛠️ Technologies Used

- Azure Event Hub
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- PySpark
- Structured Streaming
- Delta Live Tables
- Git
- SQL

## ✨ Key Features

- Built a real-time ELT data pipeline using Microsoft Azure services.
- Used Azure Event Hub for streaming event ingestion.
- Used Azure Data Factory to transfer raw streaming data into Azure Data Lake Storage (Bronze layer).
- Processed streaming data using Azure Databricks Structured Streaming and PySpark.
- Implemented Bronze, Silver, and Gold layers following the Medallion Architecture.
- Generated analytics-ready datasets for downstream analysis.

## 🔄 Project Workflow

1. Generated sample events using a dummy web application.
2. Sent streaming events to Azure Event Hub.
3. Used Azure Data Factory to ingest raw events into Azure Data Lake Storage.
4. Processed raw data using Azure Databricks Structured Streaming.
5. Applied PySpark transformations to create Silver and Gold layers.
6. Generated curated datasets for analytics.

## 📂 Repository Contents

This repository contains the sample event producer application and supporting project resources used for the real-time data engineering pipeline.

> **Note:** The sample web application was used only to generate events for Azure Event Hub. My primary contribution focused on building and understanding the Azure Data Engineering pipeline, including Azure Event Hub, Azure Data Factory, Azure Data Lake Storage, Azure Databricks Structured Streaming, and PySpark-based data processing.
