# Databricks notebook source
# MAGIC %run "./setup"

# COMMAND ----------

spark.sql(f"use {catalogo}.{schema}")

print(f"Estarei deletando todos os objetos do schema {catalogo}.{schema}")

# COMMAND ----------

# List all tables in the schema
tables_df = spark.sql(f"SHOW TABLES IN {schema}")
tables = [row.tableName for row in tables_df.collect()]

# Drop all tables in the schema
for table in tables:
    spark.sql(f"DROP TABLE {schema}.{table}")

# List all views in the schema
views_df = spark.sql(f"SHOW VIEWS IN {schema}")
views = [row.viewName for row in views_df.collect()]

# Drop all views in the schema
for view in views:
    spark.sql(f"DROP VIEW {schema}.{view}")



# COMMAND ----------

# Drop all volumes in the schema
volumes_df = spark.sql(f"SHOW VOLUMES IN {schema}")
volumes = [row.volume_name for row in volumes_df.collect()]
for volume in volumes:
    spark.sql(f"DROP VOLUME {schema}.{volume}")