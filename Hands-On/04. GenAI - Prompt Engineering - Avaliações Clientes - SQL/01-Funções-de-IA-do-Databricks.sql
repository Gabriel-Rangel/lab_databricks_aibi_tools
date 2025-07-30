-- Databricks notebook source
-- MAGIC %md <img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>
-- MAGIC
-- MAGIC # Hands-on 1/ Databricks SQL: Extraia valor dos dados com IA
-- MAGIC
-- MAGIC Nesse workshop abordaremos utilizar IA no Databricks SQL, onde analistas, engenheiros, cientistas e usuários de negocio, poderam extrair valor dos dados com IA utilizando a linguagem SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 01/ Funções de AI no Databricks SQL
-- MAGIC
-- MAGIC <img src="https://github.com/anasanchezss9/data-for-demos/blob/main/imgs/aifunctions.png?raw=true" width=100%>
-- MAGIC
-- MAGIC
-- MAGIC Temos diversas outras funções de AI no Databricks para atender demandas específicas, seguem elas abaixo:
-- MAGIC * [ai_analyze_sentiment](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_analyze_sentiment)
-- MAGIC * [ai_classify](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_classify)
-- MAGIC * [ai_extract](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_extract)
-- MAGIC * [ai_fix_grammar](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_fix_grammar)
-- MAGIC * [ai_gen](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_generate_text)
-- MAGIC * [ai_mask](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_mask)
-- MAGIC * [ai_similarity](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_similarity)
-- MAGIC * [ai_summarize](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_summarize)
-- MAGIC * [ai_translate](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/functions/ai_translate)
-- MAGIC * [ai_forcast](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/ai_forecast)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ai_analyze_sentiment
-- MAGIC
-- MAGIC A função `ai_analyze_sentiment()` permite invocar um modelo de IA generativo de última geração para realizar análise de sentimento no texto de entrada usando SQL.
-- MAGIC

-- COMMAND ----------

SELECT ai_analyze_sentiment('contratei um credito e estou no vermelho!')

-- COMMAND ----------

  SELECT ai_analyze_sentiment('consegui contratar meu credito')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ai_classify
-- MAGIC
-- MAGIC A função `ai_classify()` permite que você invoque um modelo de IA generativo de última geração para classificar o texto de entrada de acordo com os rótulos fornecidos por você usando SQL.

-- COMMAND ----------

SELECT ai_classify("Minha senha vazou.", ARRAY("urgente", "não urgente"))

-- COMMAND ----------


  SELECT ai_classify("Essa sandália é a escolha perfeita para quem busca conforto e estilo durante os dias quentes. Com um design moderno e elegante, ela é confeccionada com materiais de alta qualidade que proporcionam durabilidade e resistência.", 
  ARRAY('roupa', 'sapato', 'acessório', 'móvel'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ai_extract
-- MAGIC
-- MAGIC A função `ai_extract()` permite que você invoque um modelo de IA generativo de última geração para extrair entidades especificadas por rótulos de um determinado texto usando SQL.

-- COMMAND ----------


  SELECT ai_extract(
    'Maria mora em Nova York e trabalha para a Acme Corp, como engenheira civil,tem 25 anos, e seu documento é 41889261807.',
    array('nome', 'localização', 'empresa', 'idade', 'cpf', 'profissão' )
  );


-- COMMAND ----------


  SELECT ai_extract(
    'Envie um e-mail para jane.doe@example.com sobre a reunião às 16:00.',
    array('email', 'horário'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ai_fix_grammar
-- MAGIC
-- MAGIC A função `ai_fix_grammar()` permite invocar um modelo generativo de IA de última geração para corrigir erros gramaticais em um determinado texto usando SQL. 

-- COMMAND ----------


  SELECT ai_fix_grammar(
    'Seje mais feliz quando for em um conserto.')


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ai_mask
-- MAGIC
-- MAGIC A função `ai_mask()` permite que você invoque um modelo de IA generativo de última geração para mascarar entidades especificadas em um determinado texto usando SQL.
-- MAGIC

-- COMMAND ----------


  SELECT ai_mask(
    'Me chamo Luiza, e trabalho na Databricks, com documento 52.265.960/0001-87. Me retorne no numero 11-936655888 ou me visite em Av. Brig. Faria Lima, 3729 - BIRMANN 29 - Itaim Bibi, São Paulo - SP, 04538-133.',
    array('nome', 'endereço', 'empresa', 'cnpj', 'telefone')
  );


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ai_translate
-- MAGIC
-- MAGIC A função `ai_translate()` permite invocar um modelo de IA gerativa de última geração para traduzir texto em um idioma de destino usando SQL. 

-- COMMAND ----------


  SELECT ai_translate('Hello, how are you?', 'pt')
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ai_similarity
-- MAGIC
-- MAGIC A função `ai_similarity()` invoca um modelo de IA generativo de última geração das APIs do Databricks Foundation Model para comparar duas cadeias de caracteres e calcula a pontuação de similaridade semântica usando SQL.

-- COMMAND ----------


  SELECT ai_similarity('Apache Spark', 'Apache Spark');
 

-- COMMAND ----------


  SELECT ai_similarity('melao', 'melancia');
 

-- COMMAND ----------


  SELECT ai_similarity('melancia', 'cachorro');
 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Agora que você já aprender sobre as mais diversas funções de IA do Databricks SQL, vamos simular um caso de uso para extrairmos valor dos dados!
-- MAGIC