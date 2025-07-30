# Databricks notebook source
# MAGIC %run "./_setup/setup"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema}")

# COMMAND ----------

spark.sql(f"USE {catalogo}.{schema}")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.{schema}.landing")

volume_folder=f"/Volumes/{catalogo}/{schema}/landing"

medicos_folder = f"{volume_folder}/medicos"

# Crie a nova pasta dentro do volume
dbutils.fs.mkdirs(medicos_folder)

import requests 

def download_file(url, destination):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving '+destination+'/'+local_filename)
        with open(destination+'/'+local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename
  
url="https://raw.githubusercontent.com/Databricks-BR/lab_ai_agents/refs/heads/main/notebooks/01.%20Busca%20M%C3%A9dico/data/medicos.csv"

download_file(url, medicos_folder)

# COMMAND ----------

# Caminho do arquivo baixado
volume_folder=f"/Volumes/{catalogo}/{schema}/landing"
file_path = volume_folder + '/medicos'

# Leitura do arquivo CSV em um DataFrame Spark
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

file_path = volume_folder + '/medicos'

df_medicos = (
    spark.read
    .option('header', True)
    .option('inferSchema', True)
    .option('quote', '"')  # or "" if None returns error
    .option('escape', '"')
    .csv(file_path)
)

planos_schema = ArrayType(StringType())

df_medicos_transformed = df_medicos.withColumn(
    "planos_aceitos",
    F.from_json(F.col("planos_aceitos"), planos_schema)
)

df_medicos_transformed.write.mode("overwrite").saveAsTable("tb_medicos")



# COMMAND ----------

volume_folder=f"/Volumes/{catalogo}/{schema}/landing"

pacientes_folder = f"{volume_folder}/pacientes"

# Crie a nova pasta dentro do volume
dbutils.fs.mkdirs(pacientes_folder)

import requests 

def download_file(url, destination):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving '+destination+'/'+local_filename)
        with open(destination+'/'+local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename
  
url="https://raw.githubusercontent.com/Databricks-BR/lab_ai_agents/refs/heads/main/notebooks/01.%20Busca%20M%C3%A9dico/data/pacientes.csv"

download_file(url, pacientes_folder)

# COMMAND ----------

# Caminho do arquivo baixado
file_path = volume_folder + "/pacientes"

# Leitura do arquivo CSV em um DataFrame Spark
df_pacientes = spark.read.csv(file_path, header=True, inferSchema=True)
df_pacientes.write.mode("overwrite").saveAsTable("tb_pacientes")

# COMMAND ----------

spark.sql("""
INSERT INTO tb_pacientes (nome, cpf, endereco, plano)
VALUES ('Gabriel Rangel', 38345678911, 'Avenida Paulista, 1000 - Bela Vista, São Paulo - SP', 'Diamante');
""")

# COMMAND ----------

spark.sql("""
INSERT INTO tb_pacientes (nome, cpf, endereco, plano)
VALUES ('Ana Caroline Sanchez', 12345678910, 'Rua Alexandre Dumas, 1671 - Chácara Santo Antônio, São Paulo - SP', 'Ouro');
""")