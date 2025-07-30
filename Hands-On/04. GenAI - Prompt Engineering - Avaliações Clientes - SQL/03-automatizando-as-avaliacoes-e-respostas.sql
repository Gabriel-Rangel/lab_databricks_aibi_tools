-- Databricks notebook source
-- MAGIC %md <img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>
-- MAGIC
-- MAGIC # Hands-on 1/ Databricks SQL: Extraia valor dos dados com IA
-- MAGIC
-- MAGIC Nesse workshop abordaremos utilizar IA no Databricks SQL, onde analistas, engenheiros, cientistas e usuários de negocio, poderam extrair valor dos dados com IA utilizando a linguagem SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3/ Revisão e classificação automatizada de produtos com funções SQL
-- MAGIC
-- MAGIC
-- MAGIC Nesta estapa, exploraremos a função SQL AI `AI_GEN` para entender as avaliações do cliente e gerar uma mensagem personalizada ao cliente.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Configuração do Ambiente
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "./_setup/setup"

-- COMMAND ----------

select current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Revise a análise do cliente com a engenharia de prompt
-- MAGIC &nbsp;
-- MAGIC Os segredos para obter resultados úteis de uma IA Generativa são:
-- MAGIC - Fazer um pedido bem formulado
-- MAGIC - Ser específico sobre o tipo de resposta que você espera
-- MAGIC
-- MAGIC Para obter resultados em um formato que possamos armazenar facilmente em uma tabela, pediremos ao modelo para retornar o resultado em uma string que reflita a representação `JSON` e seja muito específico quanto ao esquema que esperamos
-- MAGIC
-- MAGIC Aqui está o prompt que desenvolvemos:
-- MAGIC ```
-- MAGIC Um cliente deixou um comentário sobre um produto. Queremos acompanhar qualquer pessoa que pareça infeliz.
-- MAGIC Extraia todas as entidades mencionadas. Para cada entidade:
-- MAGIC - classificar o sentimento como ["positivo","neutro","negativo"]
-- MAGIC - se o cliente requer acompanhamento: S ou N
-- MAGIC - motivo para exigir acompanhamento
-- MAGIC
-- MAGIC Retorne SOMENTE JSON. Nenhum outro texto fora do JSON. Formato JSON:
-- MAGIC [{
-- MAGIC      "nome_do_produto": <nome do produto>,
-- MAGIC      "sentiment": <revisar sentimento, um de ["positivo","neutro","negativo"]>,
-- MAGIC      "acompanhamento": <S ou N para acompanhamento>,
-- MAGIC      "motivo_acompanhamento": <motivo do acompanhamento>
-- MAGIC }]
-- MAGIC
-- MAGIC Avaliação:
-- MAGIC <Eu estava animado para aproveitar as ofertas da Black Friday, mas minha experiência com este cartão de crédito foi extremamente decepcionante. Durante o processo de compra, meu cartão foi bloqueado sem nenhum aviso prévio, impedindo-me de concluir minhas compras. Após contatar o suporte, levei muito tempo para resolver o problema, o que me fez perder as ofertas que eu queria. Este tipo de situação é inaceitável e me fez reconsiderar minha escolha por este cartão de crédito.>
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,Criar função enriquecer_avaliacao

CREATE OR REPLACE FUNCTION enriquecer_avaliacao(avaliacao STRING)
    RETURNS STRUCT<tipodoproduto: STRING, sentimento: STRING, acompanhamento: STRING, motivoacompanhamento: STRING>
    COMMENT 'Função extrair atributos de avaliações de clientes sobre produtos bancários. Parâmetro: avaliação. Retorna: array de structs contendo tipodoproduto, sentimento, se requer acompanhamento acompanhamento e o motivo do acompanhamento.'
    RETURN FROM_JSON(
      ai_gen(CONCAT(
      'Um cliente deixou um comentário. Acompanhamos qualquer pessoa que pareça infeliz.
          extraia as seguintes informações:
           - classificar o sentimento como ["positivo","neutro","negativo"]
           - retornar se o cliente requer acompanhamento: S ou N
           - se for necessário acompanhamento, explique qual é o motivo principal

       Dê-me apenas JSON. Nenhum texto fora do JSON. Sem adicionar: 
```json  no retorno:
          {
          "tipodoproduto": <nome do produto>,
          "sentimento": <revisar sentimento, caso o cliente reclamar de alguma coisa, mesmo que tenha algo bom em seu comentário, classifique como negativo, coloque ["positivo","negativo"]>,
          "acompanhamento": <S ou N para acompanhamento>,
          "motivoacompanhamento": <motivo do acompanhamento>
          }

          Nunca retorne null.
        
         avaliacao:', avaliacao)),
      "STRUCT<tipodoproduto: STRING, sentimento: STRING, acompanhamento: STRING, motivoacompanhamento: STRING>")

-- COMMAND ----------

  CREATE OR REPLACE TABLE avaliacoes_enriquecidas as 
    SELECT * EXCEPT (avaliacoes_enriquecidas), avaliacoes_enriquecidas.* FROM (
      SELECT *, enriquecer_avaliacao(avaliacao) AS avaliacoes_enriquecidas
        FROM avaliacoes_fake )
    INNER JOIN clientes_fake using (id_cliente)



-- COMMAND ----------

-- DBTITLE 1,Extract information from all our reviews
SELECT * FROM avaliacoes_enriquecidas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Agora vamos gerar uma resposta baseado na reclamação do cliente.

-- COMMAND ----------

  CREATE OR REPLACE FUNCTION GERE_RESPOSTAS(nome STRING, sobrenome STRING, qnt_produtos INT, produto STRING, motivo STRING)
  RETURNS STRING
      COMMENT 'Função para gerar uma resposta baseada nas informações do cliente. Parâmetros: nome, sobrenome, qnt_produtos, produto, motivo. Retorna: string contendo uma mensagem empática para o cliente.'
  RETURN ai_gen(
    CONCAT("Nosso cliente se chama ", nome, " ", sobrenome, "quem utilizou", qnt_produtos, "serviços do banco esse ano ficaram insatisfeitos com", produto,
     "especificamente devido a", motivo, ". Forneça uma mensagem empática em português do Brasil que eu possa enviar ao meu cliente
     incluindo a oferta de uma ligação com o gerente de produto relevante para deixar comentários. Eu quero reconquistar meu cliente e não quero aconteça o churn.")
  )



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Antes de aplicar, iremos testar nossa função com alguns dados de exemplo.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Let's test our response
-- MAGIC r = spark.sql('SELECT GERE_RESPOSTAS("Ana", "Silva", 235, "cartão de crédito", "teve seu cartão negado durante uma compra na black friday") AS resposta_cliente')
-- MAGIC display_answer(r.first()['resposta_cliente'])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora que testamos, salvaremos esse resultado na tabela `respostas_avaliacoes`
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE respostas_avaliacoes as 
    SELECT *,
      GERE_RESPOSTAS(nome, sobrenome, qnt_pedido, tipodoproduto, motivoacompanhamento) AS rascunho_resposta
    FROM avaliacoes_enriquecidas where acompanhamento='S'
    LIMIT 10

-- COMMAND ----------

SELECT * FROM respostas_avaliacoes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Usando Unity Catalog Tools
-- MAGIC
-- MAGIC O primeiro passo na construção do nosso agente será entender como utilizar **Unity Catalog Tools**.
-- MAGIC
-- MAGIC No laboratório anterior, criamos algumas funções, como a `enriquecer_avaliacao`, que nos permitiam facilitar a invocação dos nossos modelos de IA Generativa a partir do SQL. No entanto, essas mesmas funções também podem ser utilizadas como ferramentas por nossas LLMs. Basta indicar quais funções o modelo pode utilizar!
-- MAGIC
-- MAGIC Poder utilizar um mesmo catálogo de ferramentas em toda a plataforma simplifica bastante a nossa vida ao promover a reutilização desses ativos. Isso pode economizar horas de redesenvolvimento, bem como padronizar esses conceitos.
-- MAGIC
-- MAGIC Vamos ver como utilizar ferramentas na prática!
-- MAGIC
-- MAGIC 1. No **menu principal** à esquerda, clique em **`Playground`**
-- MAGIC 2. Clique no **seletor de modelos** e selecione o modelo **`Meta Llama 3.1 70B Instruct`** (caso já não esteja selecionado)
-- MAGIC 3. Clique em **Tools** e depois em **Add Tool** 
-- MAGIC 4. Em **Hosted Function**, digite `workshops_databricks.genai_clientes_<seu_nome>.enriquecer_avaliacao`
-- MAGIC 5. Adicione a instrução abaixo:
-- MAGIC     ```
-- MAGIC     Revise a avaliação abaixo:
-- MAGIC     Comprei um tablet e estou muito insatisfeito com a qualidade da bateria. Ela dura muito pouco tempo e demora muito para carregar.
-- MAGIC     ```
-- MAGIC 6. Clique no ícone **enviar**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Com a Tools você pode adicionar essa e mais funções e na hora que for fazer uma pergunta para seu agente ele o LLM irá escolher a função mais adequada para responder a sua pergunta. Você verá mais detalhes sobre o Unity Catalog Agent Tools mais para frente.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Parabéns, agora você já sabe como utilizar as funções de IA para trabalhar com um caso de negócio.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Sabemos que ao utilizar a função `ia_gen` sabemos que o modelo LLM por trás, é algum modelo que a Databricks escolhe, e aponta qual é em sua documentação pública. 
-- MAGIC
-- MAGIC Mas e se eu quiser escolher um modelo que está provisionado no onpremises, ou no Azure open ai, ou que eu mesmo tenha criado no Databricks... isso é possível?