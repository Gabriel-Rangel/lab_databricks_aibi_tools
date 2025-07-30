# Databricks notebook source
# MAGIC %run "../setup"

# COMMAND ----------

spark.sql(f"USE {catalogo}.{schema_volume_transacoes}")

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.{schema_volume_transacoes}.landing")

volume_folder=f"/Volumes/{catalogo}/{schema_volume_transacoes}/landing"

clientes_folder = f"{volume_folder}/clientes"

# Crie a nova pasta dentro do volume
dbutils.fs.mkdirs(clientes_folder)

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
  
url="https://raw.githubusercontent.com/anasanchezss9/data-for-demos/main/dados_clientes/dados_clientes_silver.csv"

download_file(url, clientes_folder)

# COMMAND ----------

# Caminho do arquivo baixado
file_path = volume_folder + "/clientes"

# Leitura do arquivo CSV em um DataFrame Spark
df_clientes = spark.read.csv(file_path, header=True, inferSchema=True)



# COMMAND ----------

from pyspark.sql.functions import when

df_clientes = df_clientes.withColumn('UF', 
                           when(df_clientes.UF == 'Bahia', 'BA')
                           .when(df_clientes.UF == 'São Paulo', 'SP')
                           .when(df_clientes.UF == 'Rio de Janeiro', 'RJ')
                           .otherwise(df_clientes.UF))

# COMMAND ----------

df_clientes.write.mode("overwrite").saveAsTable("tb_clientes_silver")