# Databricks notebook source
# MAGIC %md ### Data Generation

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

pip list

# COMMAND ----------

# basic faker setup

from faker import Faker
import random
from datetime import date

fake = Faker()


# COMMAND ----------

def generate_transaction():
    return {
        "transaction_id": fake.uuid4(),
        "customer_id": fake.uuid4(),
        "customer_name": fake.name(),
        "account_number": fake.iban(),
        "transaction_date": fake.date_between(start_date="-2y", end_date="today"),
        "transaction_type": random.choice(["DEBIT", "CREDIT"]),
        "amount": round(random.uniform(5.0, 5000.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "merchant": fake.company(),
        "country": fake.country(),
        "is_fraud": random.choice([0, 0, 0, 1])  # skewed toward non-fraud
    }


# COMMAND ----------

num_records = 10000

data = [generate_transaction() for _ in range(num_records)]


# COMMAND ----------

raw_df = spark.createDataFrame(data)
raw_df.display()


# COMMAND ----------

# Save as delta

raw_df.write.format("delta").mode("overwrite").save("/Volumes/databricksansh/finance/finance_volume/raw")


# COMMAND ----------

# Save as parquet

raw_df.write.format("parquet").mode("overwrite").option("header", "true").save("/Volumes/databricksansh/finance/finance_volume/raw_parquet/")


# COMMAND ----------

# Save as csv

raw_df.write.format("csv").option("header", "true").mode("overwrite").save("/Volumes/databricksansh/finance/finance_volume/raw_csv/")

# COMMAND ----------

# DBTITLE 1,Query Parquet file with read_files()
# MAGIC %sql
# MAGIC SELECT * FROM read_files('/Volumes/databricksansh/finance/finance_volume/raw/part-00000-8c406317-114d-437e-bbf7-201f7811eee3.c000.snappy.parquet', format => 'parquet')

# COMMAND ----------

# MAGIC %md ### Column names are not in CSV if headeris not true

# COMMAND ----------

# read csv

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", ",").load("/Volumes/databricksansh/finance/finance_volume/raw_csv/")

# COMMAND ----------

display(df)

# COMMAND ----------

# Read delta 

df = spark.read.format("delta").load("/Volumes/databricksansh/finance/finance_volume/raw")
display(df)


# COMMAND ----------

# Read parquet

new_df = spark.read.format("parquet").load("/Volumes/databricksansh/finance/finance_volume/raw_parquet/")
display(new_df)

# COMMAND ----------

# MAGIC %md ### Bronze Table Creation

# COMMAND ----------

bronze_df = spark.read \
  .format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .csv("/Volumes/databricksansh/finance/finance_volume/raw_csv/")


# COMMAND ----------

bronze_df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("databricksansh.finance.bronze_transactions")


# COMMAND ----------

# MAGIC %md ### Sliver Table Creation

# COMMAND ----------

from pyspark.sql.functions import col, to_date
from pyspark.sql.types import *

silver_df = spark.table("databricksansh.finance.bronze_transactions") \
  .withColumn("amount", col("amount").cast(DoubleType())) \
  .withColumn("transaction_date", to_date(col("transaction_date"))) \
  .withColumn("is_fraud", col("is_fraud").cast(IntegerType())) \
  .withColumn("transaction_type", col("transaction_type")) \
  .filter(col("amount").isNotNull())


# COMMAND ----------

silver_df = silver_df.dropDuplicates(["transaction_id"])


# COMMAND ----------

silver_df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("databricksansh.finance.silver_transactions")


# COMMAND ----------

# MAGIC %md ### Gold Table Creation

# COMMAND ----------

# MAGIC %md ### Example 1: Daily transaction metrics

# COMMAND ----------

# MAGIC %md ### Example 2: Fraud summary

# COMMAND ----------

from pyspark.sql.functions import sum, count

gold_daily_df = silver_df.groupBy("transaction_date") \
  .agg(
      count("*").alias("txn_count"),
      sum("amount").alias("total_amount")
  )


# COMMAND ----------

gold_daily_df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("databricksansh.finance.gold_daily_metrics")


# COMMAND ----------

gold_fraud_df = silver_df.groupBy("is_fraud") \
  .agg(count("*").alias("txn_count"))


# COMMAND ----------

gold_fraud_df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("databricksansh.finance.gold_fraud_metrics")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksansh.finance.bronze_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from databricksansh.finance.bronze_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from databricksansh.finance.silver_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from databricksansh.finance.gold_daily_metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricksansh.finance.gold_fraud_metrics