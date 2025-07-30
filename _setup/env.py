# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook para realizar a configuração do ambiente

# COMMAND ----------

##### Preencha com o nome do catálogo
catalogo = "databricks_workshop"

##### Preencha com o nome do prefixo do schema
prefix_db = "db_brpv_"

#### Preencher como true caso tenham um schema comum para criarmos a tabela de transacoes
criar_schema_comum = True
warehouse_name = "Workshop Databricks"