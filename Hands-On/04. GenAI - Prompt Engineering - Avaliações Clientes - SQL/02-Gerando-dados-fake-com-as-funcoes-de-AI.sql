-- Databricks notebook source
-- MAGIC %md <img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>
-- MAGIC
-- MAGIC # Hands-on 1/ Databricks SQL: Extraia valor dos dados com IA
-- MAGIC
-- MAGIC Nesse workshop abordaremos utilizar IA no Databricks SQL, onde analistas, engenheiros, cientistas e usuários de negocio, poderam extrair valor dos dados com IA utilizando a linguagem SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Aumentando a satisfação do cliente com análise automática de avaliações de clientes sobre produtos bancários
-- MAGIC
-- MAGIC Neste laboratório, construiremos um pipeline de dados que pega avaliações de clientes, na forma de texto livre, e as enriquece com informações extraídas ao fazer perguntas em linguagem natural aos modelos de IA Generativa disponíveis no Databricks. Também forneceremos recomendações para as próximas melhores ações à nossa equipe de atendimento ao cliente - ou seja, se um cliente requer acompanhamento e um rascunho de mensagem de resposta.
-- MAGIC
-- MAGIC <img src="https://github.com/anasanchezss9/data-for-demos/blob/main/imgs/pipeline_aigen.png?raw=true" width=100%>
-- MAGIC
-- MAGIC Para cada avaliação, nós:
-- MAGIC
-- MAGIC 1. Acessaos às avaliações dos cleintes
-- MAGIC 2. Identificamos o sentimento do cliente e extraímos os produtos mencionados
-- MAGIC 3.  Geramos uma resposta personalizada para o cliente
-- MAGIC
-- MAGIC <img src="https://github.com/anasanchezss9/data-for-demos/blob/main/imgs/dsql_aigen.png?raw=true" width=100%>
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2/ Gerando avaliações fake de cliente para podermos extrair os atributos da avaliação como

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Configuração do Ambiente
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "./_setup/setup"

-- COMMAND ----------

-- DBTITLE 1,Verificando se eu estou acessando o catálogo e o schema correto apresentado na célula anterior
select current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Utilizando IA Generativa com SQL
-- MAGIC Vamos rodar uma função SQL de AI simples. Pediremos ao modelo que gere um texto para uma análise de produto de bancário.
-- MAGIC
-- MAGIC *(Como alternativa, você pode copiar/colar o código SQL de uma nova consulta usando o [Databricks SQL Editor](/sql/editor/) para vê-lo em ação)*

-- COMMAND ----------

SELECT ai_gen(
  "Gere uma breve amostra de review de um cartão de crédito em português do Brasil. O cliente que escreveu o review está muito insatisfeito com o produto por causa de uma situação que foi utiliza-lo durante a black friday e seu cartão foi bloqueado e não conseguiu realizar a compra. Escreva como se fosse o cliente falando.") 
as product_review

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gerando um conjunto de dados de amostra mais completo com engenharia de prompt
-- MAGIC
-- MAGIC Agora que sabemos como enviar uma consulta básica ao modelo usando funções SQL, vamos fazer um pedido mais detalhada ao modelo.
-- MAGIC
-- MAGIC Pediremos diretamente ao modelo para gerar várias linhas e retornar diretamente como um json.
-- MAGIC
-- MAGIC Aqui está um exemplo rápido para gerar JSON:
-- MAGIC ```
-- MAGIC Gere um conjunto de dados de amostra de 2 linhas que contenha as seguintes colunas: "data" (datas aleatórias em 2022),
-- MAGIC        "review_id" (id aleatório), "id_cliente" (aleatório de 1 a 100) e "review". As avaliações devem imitar análises úteis de produtos
-- MAGIC        deixado em um site de um banco, eles tem diversos produtos, tais como: cartão de crédito; seguro de residencia, carro, celular; finciamento; empréstimo e conta corrente.
-- MAGIC       
-- MAGIC        As avaliações devem ser sobre os produtos bancários e em português do Brasil.
-- MAGIC
-- MAGIC        As revisões devem variar em extensão (menor: uma frase, mais longa: 2 parágrafos), sentimento e complexidade. Uma revisão muito complexa falaria sobre vários tópicos (entidades) sobre o produto com sentimentos variados por tópico. Forneça uma mistura de aspectos positivos, negativos, e comentários neutros.
-- MAGIC
-- MAGIC        Dê-me apenas JSON. Nenhum texto fora do JSON. 
-- MAGIC       [{"review_date":<DATE>, "review_id":<long>, "id_cliente":<long>, "review":<STRING>}]
-- MAGIC ```

-- COMMAND ----------

SELECT ai_gen(
      'Gere um conjunto de dados de amostra de 2 linhas que contenha as seguintes colunas: "data" (datas aleatórias em 2022),
       "review_id" (id aleatório), "id_cliente" (aleatório de 1 a 100) e "review". As avaliações devem imitar análises úteis de produtos
       deixado em um site de um banco, eles tem diversos produtos, tais como: cartão de crédito; seguro de residencia, carro, celular; finciamento; empréstimo e conta corrente.
      
       As avaliações devem ser sobre os produtos bancários e em português do Brasil.

       As revisões devem variar em extensão (menor: uma frase, mais longa: 2 parágrafos), sentimento e complexidade. Uma revisão muito complexa falaria sobre vários tópicos (entidades) sobre o produto com sentimentos variados por tópico. Forneça uma mistura de aspectos positivos, negativos, e comentários neutros.

       Dê-me apenas JSON. Nenhum texto fora do JSON. Sem adicionar: ```json no retorno. Remova os seguintes caracteres ```
      [{"review_date":<DATE>, "review_id":<long>, "id_cliente":<long>, "review":<STRING>}]') as reviews

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ### Criando uma função para simplificar a chamada e compartilhar esse prompt criado com outros colegas
-- MAGIC
-- MAGIC Quando terminarmos o nosso prompt e esse funcionar com sucesso, criaremos uma função SQL wrapper `GERE_AVALIACOES_fake` para salvarmos no Unity Catalog e compartilharmos com nossos colegas para ser reutilizada em outros projetos.
-- MAGIC
-- MAGIC <img src="https://github.com/anasanchezss9/data-for-demos/blob/main/imgs/sqlfunc.png?raw=true" width=100%>

-- COMMAND ----------

CREATE OR REPLACE FUNCTION GERE_AVALIACOES_fake(num_reviews INT DEFAULT 5)
RETURNS array<struct<data_avaliacao:date, id_avaliacao:long, id_cliente:long, avaliacao:string>>
COMMENT "Função para gerar avaliações fakes de avaliações de clientes sobre produtos bancários. Parâmetro: num_reviews (número de avaliações a serem geradas, padrão é 5).   Retorna: array de structs contendo data_avaliacao, id_avaliacao, id_cliente e avaliacao."
RETURN 
SELECT FROM_JSON(
    ai_gen(
    CONCAT('Gere um conjunto de dados de amostra de ', num_reviews, ' linhas que contenha as seguintes colunas: "data" (datas aleatórias em 2022),
       "id_avaliacao" (id aleatório), "id_cliente" (ordenado de 1 a 100) e "avaliacao". As avaliações devem imitar análises úteis de produtos
       deixado em um site de um banco, eles tem diversos produtos, tais como: cartão de crédito; seguro de residencia, carro, celular; finciamento; empréstimo e conta corrente.
      
       As avaliações devem ser sobre os produtos bancários e em português do Brasil.

       As revisões devem variar em extensão (menor: uma frase, mais longa: 2 parágrafos), sentimento e complexidade. Forneça uma mistura de tipos de avaliações, algumas com aspectos positivos e outras com aspectos negativos.

       Dê-me apenas JSON. Nenhum texto fora do JSON. Sem adicionar: ```json no retorno. Remova os seguintes caracteres ```
      [{"data_avaliacao":<DATE>, "id_avaliacao":<long>, "id_cliente":<long>, "avaliacao":<STRING>}]')), 
      "array<struct<data_avaliacao:date, id_avaliacao:long, id_cliente:long, avaliacao:string>>")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"Link da sua função GERE_AVALIACOES_fake: https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/explore/data/functions/{catalogo}/{schema}/GERE_AVALIACOES_fake")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Convertendo os resultados de json para dataframe
-- MAGIC
-- MAGIC Nossos resultados parecem bons. Tudo o que precisamos fazer agora é transformar os resultados do texto em JSON e explodir os resultados em N linhas de um dataframe.
-- MAGIC
-- MAGIC Vamos criar uma nova função para fazer isso:

-- COMMAND ----------

-- DBTITLE 1,Explode the json result as a table
SELECT review.* FROM (
      SELECT explode(reviews) as review FROM (
       SELECT GERE_AVALIACOES_fake(5) as reviews))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Salvando nosso conjunto de dados como uma tabela para ser usada diretamente em nossa demonstração.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,Save the crafted review as a new table
CREATE OR REPLACE TABLE avaliacoes_fake
COMMENT "Dados brutos de avaliacoes fake criados a partir da função GERE_AVALIACOES_fake"
AS
SELECT review.* FROM (
  SELECT explode(reviews) as review FROM (
    SELECT GERE_AVALIACOES_fake(15) as reviews))

-- COMMAND ----------

select * from avaliacoes_fake

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"Link da sua tabela avaliacoes_fake: https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/explore/data/{catalogo}/{schema}/avaliacoes_fake")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Além disso, vamos gerar alguns clientes utilizando a mesma ideia:

-- COMMAND ----------

-- DBTITLE 1,Além disso, vamos gerar alguns usuários utilizando a mesma ideia:

CREATE OR REPLACE FUNCTION GERE_CLIENTES_fake(num_reviews INT DEFAULT 10)
RETURNS array<struct<id_cliente:long, nome:string, sobrenome:string, qnt_pedido:int>>
RETURN 
SELECT FROM_JSON(
    ai_gen(
      CONCAT('Gere um conjunto de dados de amostra de clientes brasileiros ', num_reviews,' contendo as seguintes colunas:
       "id_cliente" (long from 1 to ', num_reviews, '), "nome", "sobrenome" e qnt_pedido (número positivo aleatório, menor que 200)

       Dê-me apenas JSON. Nenhum texto fora do JSON. Sem adicionar: ```json  no retorno.
      [{"id_cliente":<long>, "nome":<string>, "sobrenome":<string>, "qnt_pedido":<int>}]')), 
      "array<struct<id_cliente:long, nome:string, sobrenome:string, qnt_pedido:int>>");


CREATE OR REPLACE TABLE clientes_fake
COMMENT "Raw customers"
AS
SELECT customer.* FROM (
  SELECT explode(customers) as customer FROM (
    SELECT GERE_CLIENTES_FAKE(15) as customers));



-- COMMAND ----------

select * from clientes_fake

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Próximos passos
-- MAGIC Agora estamos prontos para implementar nosso pipeline para extrair informações de nossas análises! Abra [03-revisão e resposta automatizada de produto]($./03-automatizando-as-avaliacoes-e-respostas) para continuar.
-- MAGIC
-- MAGIC
-- MAGIC Volte para [a introdução]($./README.md)