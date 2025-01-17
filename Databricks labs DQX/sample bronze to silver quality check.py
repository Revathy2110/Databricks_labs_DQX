# Databricks notebook source
# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC **Sample data 1**

df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/shared_uploads/revathy.s@diggibyte.com/tmp/*.csv")

# COMMAND ----------

def rename_columns(df):
    return df.toDF(*[col.lower().replace(" ", "_") for col in df.columns])

df = rename_columns(df)

# COMMAND ----------

bronze_path = "/tmp/dqx_demo/bronze"
df_bronze = df.write.format("delta").mode("overwrite").save(bronze_path)

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.sdk import WorkspaceClient
import yaml

bronze = spark.read.format("delta").load(bronze_path)

ws = WorkspaceClient()
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(bronze)
print(summary_stats)
print(profiles)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"
print(yaml.safe_dump(checks))

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- check:
    arguments:
      col_name: customer_id
    function: is_not_null
  criticality: error
  name: customer_id_is_null
- check:
    arguments:
      col_name: name
    function: is_not_null
  criticality: error
  name: name_is_null
- check:
    arguments:
      col_name: email_id
    function: is_not_null
  criticality: error
  name: email_id_is_null
- check:
    arguments:
      col_name: address
    function: is_not_null
  criticality: error
  name: address_is_null
- check:
    arguments:
      col_name: gender
    function: is_not_null
  criticality: error
  name: gender_is_null
- check:
    arguments:
      allowed:
      - female
      - male
      col_name: gender
    function: value_is_in_list
  criticality: error
  name: gender_other_value
- check:
    arguments:
      col_name: age
    function: is_not_null
  criticality: error
  name: age_is_null
- check:
    arguments:
      col_name: joining_date
    function: is_not_null
  criticality: error
  name: joining_date_is_null
- check:
    arguments:
      col_name: registered
    function: is_not_null
  criticality: error
  name: registered_is_null
- check:
    arguments:
      col_name: order_id
      trim_strings: true
    function: is_not_null_and_not_empty
  criticality: error
  name: order_id_is_null_or_empty
- check:
    arguments:
      col_name: orders
    function: is_not_null
  criticality: error
  name: orders_is_null
- check:
    arguments:
      col_name: spent
    function: is_not_null
  criticality: error
  name: spent_is_null
- check:
    arguments:
      col_name: job
    function: is_not_null
  criticality: error
  name: job_is_null
- check:
    arguments:
      col_name: hobbies
    function: is_not_null
  criticality: error
  name: hobbies_is_null
- check:
    arguments:
      col_name: is_married
    function: is_not_null
  criticality: error
  name: is_married_is_null
- check:
    arguments:
      allowed:
      - 'FALSE'
      - 'TRUE'
      col_name: is_married
    function: value_is_in_list
  criticality: error
  name: is_married_other_value
""")


dq_engine = DQEngine(WorkspaceClient())
status = dq_engine.validate_checks(checks)
print(status.has_errors)
print(status.errors)

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Apply checks when processing to silver layer
bronze = spark.read.format("delta").load(bronze_path)
silver, quarantine = dq_engine.apply_checks_by_metadata_and_split(bronze, checks)

# COMMAND ----------

display(silver)

# COMMAND ----------

display(quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC **Sample data 2**

# COMMAND ----------

df2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("dbfs:/FileStore/shared_uploads/revathy.s@diggibyte.com/tmp/20240107_sales_data-1.csv")

# COMMAND ----------

display(df2)

# COMMAND ----------

def rename_columns(df):
    return df.toDF(*[col.lower().replace(" ", "_") for col in df.columns])

df2 = rename_columns(df2)

# COMMAND ----------

bronze_path = "/tmp/dqx_demo/bronze2"
df_bronze_2 = df2.write.format("delta").mode("overwrite").save(bronze_path)

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.labs.dqx.profiler.dlt_generator import DQDltGenerator
from databricks.sdk import WorkspaceClient
import yaml

bronze_path = "/tmp/dqx_demo/bronze2"
bronze_2 = spark.read.format("delta").load(bronze_path)

ws = WorkspaceClient()
profiler = DQProfiler(ws)
summary_stats, profiles = profiler.profile(bronze)
print(summary_stats)
print(profiles)

# generate DQX quality rules/checks
generator = DQGenerator(ws)
checks = generator.generate_dq_rules(profiles)  # with default level "error"
print(yaml.safe_dump(checks))

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

checks = yaml.safe_load("""
- check:
    arguments:
      col_name: order_date
    function: is_not_null
  criticality: error
  name: order_date_is_null
- check:
    arguments:
      col_name: category
    function: is_not_null
  criticality: error
  name: category_is_null
- check:
    arguments:
      col_name: city
    function: is_not_null
  criticality: error
  name: city_is_null
- check:
    arguments:
      col_name: country
    function: is_not_null
  criticality: error
  name: country_is_null
- check:
    arguments:
      col_name: customer_id
    function: is_not_null
  criticality: error
  name: customer_id_is_null
- check:
    arguments:
      col_name: order_id
    function: is_not_null
  criticality: error
  name: order_id_is_null
- check:
    arguments:
      col_name: postal_code
    function: is_not_null
  criticality: error
  name: postal_code_is_null
- check:
    arguments:
      col_name: product_id
    function: is_not_null
  criticality: error
  name: product_id_is_null
- check:
    arguments:
      col_name: profit
    function: is_not_null
  criticality: error
  name: profit_is_null
- check:
    arguments:
      col_name: quantity
    function: is_not_null
  criticality: error
  name: quantity_is_null
- check:
    arguments:
      col_name: region
    function: is_not_null
  criticality: error
  name: region_is_null
- check:
    arguments:
      col_name: sales
    function: is_not_null
  criticality: error
  name: sales_is_null
- check:
    arguments:
      col_name: segment
    function: is_not_null
  criticality: error
  name: segment_is_null
- check:
    arguments:
      col_name: ship_date
    function: is_not_null
  criticality: error
  name: ship_date_is_null
- check:
    arguments:
      col_name: ship_mode
    function: is_not_null
  criticality: error
  name: ship_mode_is_null
- check:
    arguments:
      col_name: state
    function: is_not_null
  criticality: error
  name: state_is_null
- check:
    arguments:
      col_name: latitude
    function: is_not_null
  criticality: error
  name: latitude_is_null
- check:
    arguments:
      col_name: longitude
    function: is_not_null
  criticality: error
  name: longitude_is_null
""")


dq_engine = DQEngine(WorkspaceClient())
status = dq_engine.validate_checks(checks)
print(status.has_errors)
print(status.errors)

# COMMAND ----------

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

dq_engine = DQEngine(WorkspaceClient())

# Apply checks when processing to silver layer
bronze_path = "/tmp/dqx_demo/bronze2"
bronze_2 = spark.read.format("delta").load(bronze_path)
silver, quarantine = dq_engine.apply_checks_by_metadata_and_split(bronze_2, checks)

# COMMAND ----------

display(silver)

# COMMAND ----------

display(quarantine)

