"""Entrypoint for the data processing and visualization with Spark
NOTE: Replace the file names with the URLs when moving this to an actual Spark
Session in Azure
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType, FloatType
)
import pandas as pd


spark = SparkSession.builder.getOrCreate()

ASSINATURAS_FILE_NAME = "data/assinaturas.csv"
TRANSACOES_FILE_NAME = "data/transacoes.csv"
PRODUTO_FILE_NAME = "data/produto.csv"
USUARIOS_FILE_NAME = "data/usuarios.csv"
METODO_PAGAMENTO_FILE_NAME = "data/metodo_pagamento.csv"
CONFIGURACAO_TARIFACAO_FILE_NAME = "data/configuracao_tarifacao.csv"

ASSINATURAS_TABLE_NAME = "assinaturas"
TRANSACOES_TABLE_NAME = "transacoes"
PRODUTO_TABLE_NAME = "produto"
USUARIOS_TABLE_NAME = "usuarios"
METODO_PAGAMENTO_TABLE_NAME = "metodo_pagamento"
CONFIGURACAO_TARIFACAO_TABLE_NAME = "configuracao_tarifacao"

# assinaturas data
assinaturas_schema = StructType(
    [
        StructField("id_assinatura", IntegerType(), False),
        StructField("codigo_usuario", StringType(), False),
        StructField("cod_produto", IntegerType(), False),
        StructField("data_assinatura", DateType(), False),
        StructField("data_cancelamento", DateType(), True),
        StructField("proxima_tarifacao", DateType(), True),
        StructField("status_assinatura", StringType(), False),
    ]
)
df_assinaturas = spark.read.csv(
    ASSINATURAS_FILE_NAME, sep=";", header=True, schema=assinaturas_schema
)
df_assinaturas.createOrReplaceTempView(ASSINATURAS_TABLE_NAME)

# transacoes data
transacoes_schema = StructType(
    [
        StructField("id_transacao", IntegerType(), False),
        StructField("id_assinatura", IntegerType(), False),
        StructField("cod_produto", IntegerType(), False),
        StructField("step_tarifacao", IntegerType(), False),
        StructField("data_transacao", DateType(), True),
        StructField("cod_metodo", IntegerType(), True),
        StructField("status_transacao", IntegerType(), False),
    ]
)
df_transacoes = spark.read.csv(
    TRANSACOES_FILE_NAME, sep=";", header=True, schema=transacoes_schema
)
df_transacoes.createOrReplaceTempView(TRANSACOES_TABLE_NAME)

# produto data
produto_schema = StructType(
    [
        StructField("cod_produto", IntegerType(), False),
        StructField("nome_produto", StringType(), False),
        StructField("descricao_produto", StringType(), False),
        StructField("data_criacao", DateType(), False),
    ]
)
df_produto = spark.read.csv(
    PRODUTO_FILE_NAME, sep=";", header=True, schema=produto_schema
)
df_produto.createOrReplaceTempView(PRODUTO_TABLE_NAME)

# usuarios data
usuarios_schema = StructType(
    [
        StructField("codigo_usuario", IntegerType(), False),
        StructField("Celular", StringType(), False),
        StructField("genero", StringType(), False),
        StructField("idade", IntegerType(), False),
        StructField("cidade", StringType(), False),
        StructField("estado", StringType(), False),
    ]
)
df_usuarios = spark.read.csv(
    USUARIOS_FILE_NAME, sep=";", header=True, schema=usuarios_schema
)
df_usuarios.createOrReplaceTempView(USUARIOS_TABLE_NAME)


# metodo_pagamento data
metodo_pagamento_schema = StructType(
    [
        StructField("cod_metodo", IntegerType(), False),
        StructField("metodo", StringType(), False),
    ]
)
df_metodo_pagamento = spark.read.csv(
    METODO_PAGAMENTO_FILE_NAME,
    sep=";",
    header=True,
    schema=metodo_pagamento_schema,
)
df_metodo_pagamento.createOrReplaceTempView(METODO_PAGAMENTO_TABLE_NAME)


# configuracao_tarifacao data
# This data has a `valor_step` column with a comma (",") as decimal separator
# I coult not find a way for Pyspark to handle that, so the suggested approach
# is to first load it into a Pandas dataframe, save the handled data, and
# import it again
df_configuracao_tarifacao_pandas = pd.read_csv(
    CONFIGURACAO_TARIFACAO_FILE_NAME, sep=";", decimal=","
)
configuracao_tarifacao_treated_file_name = "configuracao_tarifacao_treated.csv"
df_configuracao_tarifacao_pandas.to_csv(
    configuracao_tarifacao_treated_file_name, index=False, sep=";"
)

# Read the treated data
configuracao_tarifacao_schema = StructType(
    [
        StructField("cod_produto", IntegerType(), False),
        StructField("step_tarifacao", IntegerType(), False),
        StructField("descricao_step", StringType(), False),
        StructField("valor_step", FloatType(), False),
        StructField("dias_utilizacao", IntegerType(), False),
    ]
)
df_configuracao_tarifacao = spark.read.csv(
    configuracao_tarifacao_treated_file_name,
    sep=";",
    header=True,
    schema=configuracao_tarifacao_schema,
)
df_configuracao_tarifacao.createOrReplaceTempView(
    CONFIGURACAO_TARIFACAO_TABLE_NAME
)


# Answering questions
# 1. Total de assinaturas ativas por produto
# Se `data_cancelamento` ?? nulo, a assinatura est?? ativa
df_1 = spark.sql(
    f"""
    SELECT
        cod_produto, COUNT(*) AS num_active_signatures
    FROM {ASSINATURAS_TABLE_NAME}
    WHERE data_cancelamento IS NULL
    GROUP BY cod_produto
    """
)
# df_1.show()

# 2. Total de assinaturas canceladas por produto
# A l??gica ?? o oposto da anterior
df_2 = spark.sql(
    f"""
    SELECT
        cod_produto, COUNT(*) AS num_canceled_signatures
    FROM {ASSINATURAS_TABLE_NAME}
    WHERE data_cancelamento IS NOT NULL
    GROUP BY cod_produto
    """
)
# df_2.show()

# 3. Total de usu??rios que nunca assinaram nenhum servi??o
# Use uma subquery para buscar os c??digos de usu??rio da tabela de assinaturas
# e depois filtre da tabela de usu??rios aquels que n??o estiverem l??
df_3 = spark.sql(
    f"""
    SELECT
        COUNT(codigo_usuario) AS num_users_without_signature
    FROM {USUARIOS_TABLE_NAME}
    WHERE codigo_usuario NOT IN (
        SELECT
            DISTINCT codigo_usuario
        FROM {ASSINATURAS_TABLE_NAME}
    )
    """
)
# df_3.show()

# 4. Total de usu??rios que possuem (ou j?? tiveram) mais de um servi??o assinado
# Em uma sub-query buscamos todos os usu??rios com mais de um servi??o, i.e.,
# aqueles cujo c??digo aparece mais de uma vez. Em seguida, buscamos o total
# de linhas nesta sub-query
df_4 = spark.sql(
    f"""
    SELECT
        COUNT(codigo_usuario)
    FROM (
        SELECT
            codigo_usuario, COUNT(*)
        FROM {ASSINATURAS_TABLE_NAME}
        GROUP BY codigo_usuario
        HAVING COUNT(*) > 1
    )
    """
)
# df_4.show()

# 5. Quantidade de assinaturas ativas por regi??o geogr??fica
df_5 = spark.sql(
    f"""
    SELECT
        COUNT(*) AS num_active_signatures, usuarios.cidade, usuarios.estado
    FROM {ASSINATURAS_TABLE_NAME} assinaturas
    INNER JOIN {USUARIOS_TABLE_NAME} usuarios ON assinaturas.codigo_usuario = usuarios.codigo_usuario
    WHERE assinaturas.data_cancelamento IS NULL
    GROUP BY usuarios.cidade, usuarios.estado
    """
)
# df_5.show()

# 6. Tarifa????es por regi??o geogr??fica
# A query abaixo mostra os valores de tarifa????o agrupados por cidade e estado
df_6 = spark.sql(
    f"""
    SELECT
        configuracao_tarifacao.valor_step, usuarios.cidade, usuarios.estado
    FROM {ASSINATURAS_TABLE_NAME} assinaturas
    INNER JOIN {USUARIOS_TABLE_NAME} usuarios ON assinaturas.codigo_usuario = usuarios.codigo_usuario
    INNER JOIN {CONFIGURACAO_TARIFACAO_TABLE_NAME} configuracao_tarifacao ON assinaturas.cod_produto = configuracao_tarifacao.cod_produto
    GROUP BY configuracao_tarifacao.valor_step, usuarios.cidade, usuarios.estado
    ORDER BY usuarios.estado, usuarios.cidade
    """
)
# df_6.show()

# 7. Quais s??o os produtos com as maiores receitas
# Primeira busca determinando receita total por produto e por step
df_receitas_por_produto_por_step = spark.sql(
    f"""
    SELECT
        configuracao_tarifacao.cod_produto,
        configuracao_tarifacao.step_tarifacao,
        SUM(configuracao_tarifacao.valor_step) AS value_per_product_per_step
    FROM {TRANSACOES_TABLE_NAME} transacoes
    INNER JOIN {CONFIGURACAO_TARIFACAO_TABLE_NAME} configuracao_tarifacao
    ON (
        configuracao_tarifacao.cod_produto = transacoes.cod_produto
        AND configuracao_tarifacao.step_tarifacao = transacoes.step_tarifacao
    )
    GROUP BY configuracao_tarifacao.cod_produto, configuracao_tarifacao.step_tarifacao
    ORDER BY configuracao_tarifacao.cod_produto, configuracao_tarifacao.step_tarifacao
    """
)

df_receitas_por_produto_por_step.show()
df_receitas_por_produto_por_step.createOrReplaceTempView("receita_por_produto_por_step")

# Agrupa a receita por produto
df_receitas_por_produto = spark.sql(
    """
    SELECT
        cod_produto, SUM(value_per_product_per_step) AS value_per_product
    FROM receita_por_produto_por_step
    GROUP BY cod_produto
    ORDER BY cod_produto
    """
)

df_receitas_por_produto.createOrReplaceTempView("receita_por_produto")
df_receitas_por_produto.show()

# Com isso, os produtos com maiores e menores receitas s??o f??ceis de serem
# determinados
df_min_receita = spark.sql(
    """
    SELECT
        cod_produto, value_per_product
    FROM receita_por_produto
    ORDER BY value_per_product ASC
    LIMIT 1
    """
)
df_min_receita.show()

df_max_receita = spark.sql(
    """
    SELECT
        cod_produto, value_per_product
    FROM receita_por_produto
    ORDER BY value_per_product DESC
    LIMIT 1
    """
)
df_max_receita.show()


# 10. Percentual dos m??todos de pagamento por produto e m??s (usu??rios pagam mais
# com cart??o de cr??dito, d??bito, PicPay ou mercado pago?)
df_transacoes_por_produto_por_metodo = spark.sql(
    f"""
    SELECT
        transacoes.cod_produto,
        metodo_pagamento.metodo,
        COUNT(*) AS num_transactions
    FROM {TRANSACOES_TABLE_NAME} transacoes
    INNER JOIN {METODO_PAGAMENTO_TABLE_NAME} metodo_pagamento
    ON  transacoes.cod_metodo = metodo_pagamento.cod_metodo
    GROUP BY transacoes.cod_produto, metodo_pagamento.metodo
    ORDER BY transacoes.cod_produto, metodo_pagamento.metodo
    """
)

df_transacoes_por_produto_por_metodo.show()

# O restante das opera????es ?? melhor feito em Pandas
df_transactions = df_transacoes_por_produto_por_metodo.toPandas()
df_transactions["num_transactions_percentage"] = (
    df_transactions["num_transactions"]
    / df_transactions.groupby(
        by="cod_produto"
    )["num_transactions"].transform("sum")
)

import ipdb; ipdb.set_trace()
