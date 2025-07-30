-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Constraints - PK e FK
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "../setup"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"USE {catalogo}.{schema_volume_transacoes}")

-- COMMAND ----------

select * from tb_clientes_silver limit 10

-- COMMAND ----------

select * from tb_transacoes_silver limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Constraints

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### NOT NULL

-- COMMAND ----------

ALTER TABLE tb_clientes_silver
ALTER COLUMN CodigoCliente SET NOT NULL

-- COMMAND ----------

ALTER TABLE tb_transacoes_silver
ALTER COLUMN id_transacao SET NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Primary Key

-- COMMAND ----------


ALTER TABLE tb_clientes_silver
ADD CONSTRAINT CodigoCliente_pk PRIMARY KEY (CodigoCliente);

-- COMMAND ----------

ALTER TABLE tb_transacoes_silver
ADD CONSTRAINT id_transacao_pk PRIMARY KEY (id_transacao);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Foreign Key

-- COMMAND ----------

ALTER TABLE tb_transacoes_silver
ADD CONSTRAINT fk_id_cliente
FOREIGN KEY (id_cliente)
REFERENCES tb_clientes_silver(CodigoCliente);