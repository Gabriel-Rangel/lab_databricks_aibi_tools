-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Criando uma Sala do Genie
-- MAGIC
-- MAGIC Para criar uma sala do Genie com as tabelas `tb_transacoes_silver` e `tb_clientes_silver`, siga os passos abaixo:
-- MAGIC
-- MAGIC 1. **Acesse o Genie**:
-- MAGIC    - No menu lateral, em **`SQL`** clique em **`Genie`**.
-- MAGIC
-- MAGIC 2. **Crie uma nova sala**:
-- MAGIC    - Clique no botão **`+ New`**.
-- MAGIC
-- MAGIC 3. **Adicione as tabelas**:
-- MAGIC    - Um menu para selecionar as bases irá aparecer, selecione as tabelas `tb_transacoes_silver` _(workshops_databricks.db_workshop_commom.tb_transacoes_silver)_ e `tb_clientes_silver`_ (workshops_databricks.db_workshop_commom.tb_clientes_silver)_.
-- MAGIC
-- MAGIC 4. **Configure a sala**:
-- MAGIC    Do lado esquerdo procure pelo item **`Settings`**
-- MAGIC    - **Title**: Dê um nome à sua sala, por exemplo, `Sala de Análise de Transações e Clientes - SEU NOME`.
-- MAGIC    - **Description**: Adicione uma descrição para a sala, como `Sala para análise das tabelas de transações e clientes`.
-- MAGIC
-- MAGIC 5. **Adicionar o Warehouse**:
-- MAGIC    - Em **`Default warehouse`** selecione o warehouse que contenha em seu nome a palavra `Treinamento ou Workshop`.
-- MAGIC
-- MAGIC Pronto! Sua sala do Genie com as tabelas `tb_transacoes_silver` e `tb_clientes_silver` está configurada.
-- MAGIC
-- MAGIC ![](https://github.com/anasanchezss9/db_sql_lab/blob/main/images/genie.png?raw=true)
-- MAGIC
-- MAGIC  Seguem abaixo algumas perguntas de exemplo para vocês começarem a conversar com os seus dados:
-- MAGIC
-- MAGIC - descreva os datasets
-- MAGIC - Quantas transações tivemos ontem?
-- MAGIC - quantas transações tivemos no ultimo mes?
-- MAGIC - qual são os 10 clientes que mais transacionam?
-- MAGIC - Qual a soma do valor transacionado por categoria de lojista e central?
-- MAGIC - quantos clientes com mais de 5 anos de relacionamento tem propensao a evasao em campinas?
-- MAGIC - quais sao esses clientes?
-- MAGIC - ordene esses clientes por maior renda mensal
-- MAGIC - qual é o canal preferencial desses clientes?
-- MAGIC - me de as informacoes sobre o cliente 9583 sobre indice de satisfacao, quantidade de atendimentos e canal preferencial
-- MAGIC - qual é o email dele?
-- MAGIC - qual é o nome do cliente que mais tem transações no mes passado
-- MAGIC - ela tem propensão a evasao?
-- MAGIC - quais são as lojas que ele mais transacional
-- MAGIC
-- MAGIC **Notem** que, mesmo sem muito contexto, a Genie já conseguiu:
-- MAGIC - Inferir quais as tabelas e colunas relevantes para responder nossas perguntas
-- MAGIC - Aplicar filtros e agregações
-- MAGIC - Responder perguntas adicionais sobre uma resposta anterior
-- MAGIC - Combinar diferentes tabelas
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. **Adicionando Instruções** </br>
-- MAGIC     Existem maneiras de complementar o conhecimento que a **Genie** já possui, uma delas é fazer o uso de instruções.</br>
-- MAGIC
-- MAGIC     **INSTRUÇÕES** são nada mais que um conjunto de sentenças em linguagem natural que podem explicar para a Genie informações importantes como:
-- MAGIC     - Significado de abreviações e termos técnicos comumente utilizadas na sua empresa
-- MAGIC     - Formato do dado (por exemplo, se os registros estão em maiúsculas ou minúsculas)
-- MAGIC     - Tratamentos necessários para determinados campos
-- MAGIC
-- MAGIC     Vamos ver como funciona:
-- MAGIC
-- MAGIC     * Faça a pergunta:
-- MAGIC       - `Quais são os clientes ESTILO ?`
-- MAGIC     
-- MAGIC     * Ops, a Genie não conseguiu responder, o que podemos fazer ?
-- MAGIC
-- MAGIC     * Do lado direito dentro da aba *Context*, clique em *Instructions* e adicione as instruções abaixo e clique em *Save*:
-- MAGIC       - ```
-- MAGIC         * Responda em português
-- MAGIC         * Classifique cada cliente em uma das seguintes categorias:
-- MAGIC             1. **ESTILO:** renda mensal > 10.000
-- MAGIC             2. **PRIVATE:** 5.000 < renda mensal <= 10.000
-- MAGIC             3. **REGULAR:** renda mensal <= 5.000
-- MAGIC         ```
-- MAGIC     
-- MAGIC     * Faça novamente a pergunta anterior
-- MAGIC
-- MAGIC     Pronto! Agora a Genie já pode responder perguntas sobre categorias de clientes dentro do contexto passado.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. **Exemplo de queries** </br>
-- MAGIC
-- MAGIC Em alguns casos, precisamos fazer cruzamentos e cálculos bastante complexos para conseguir responder às nossas perguntas e a Genie pode não entender como montar todo o racional necessário.
-- MAGIC
-- MAGIC Nesses casos, podemos fornecer exemplos de queries validadas e certificadas pelos times responsáveis. Este também é um mecanismo interessante para garantir a acurácia das respostas.
-- MAGIC
-- MAGIC Vamos ver como funciona:
-- MAGIC
-- MAGIC 1. Faça a pergunta:
-- MAGIC     - Calcule a quantidade de transações por janela móvel de 3 meses 
-- MAGIC
-- MAGIC 2. Aqui a Genie já até fez uma soma trimestre a trimestre, porém não ficou exatamente do jeito que nós gostaríamos.</br>
-- MAGIC     Então, adicione um exemplo de query seguindo os passos abaixo:
-- MAGIC     - Clique em `SQL Queries`
-- MAGIC     - Depois clique em `Add`
-- MAGIC     - Insira a pergunta anterior no campo superior
-- MAGIC     - Insira a query abaixo no campo inferior
-- MAGIC         - ```
-- MAGIC           SELECT 
-- MAGIC             window.end AS data_final_janela,
-- MAGIC             COUNT(id_transacao) AS quantidade_transacoes
-- MAGIC           FROM workshops_databricks.db_workshop_commom.tb_transacoes_silver
-- MAGIC           GROUP BY WINDOW(to_timestamp(timestamp), '90 days', '1 day')
-- MAGIC           ORDER BY data_final_janela
-- MAGIC           ```
-- MAGIC   
-- MAGIC 3. Faça novamente a pergunta. </br>
-- MAGIC    Agora sim obtivemos uma janela móvel de 3 meses que avança dia a dia.
-- MAGIC
-- MAGIC