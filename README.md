# Databricks-Finance-DLT-Project
This project implements an end-to-end finance data platform using Databricks Delta Live Tables (DLT) and the Medallion Architecture. The pipeline ingests raw transaction data into a Bronze layer, applies data quality checks and transformations in the Silver layer, and produces business-ready Gold tables for daily financial KPIs and fraud analytics.

# Overview
This project implements a Delta Live Tables (DLT) pipeline for finance transaction data using the Medallion architecture.

# Architecture
- Bronze: Raw transaction ingestion
- Silver: Cleaned and validated transactions
- Gold: Business and fraud metrics

# Tech Stack
- Databricks
- Delta Live Tables
- PySpark

# How to Run
1. Convert .py file into databricks notebook.
