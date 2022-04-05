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
# Se `data_cancelamento` é nulo, a assinatura está ativa
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
# A lógica é o oposto da anterior
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

# 3. Total de usuários que nunca assinaram nenhum serviço
# Use uma subquery para buscar os códigos de usuário da tabela de assinaturas
# e depois filtre da tabela de usuários aquels que não estiverem lá
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

# 4. Total de usuários que possuem (ou já tiveram) mais de um serviço assinado
# Em uma sub-query buscamos todos os usuários com mais de um serviço, i.e.,
# aqueles cujo código aparece mais de uma vez. Em seguida, buscamos o total
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

# 5. Quantidade de assinaturas ativas por região geográfica
df_5 = spark.sql(
    f"""
    SELECT
        COUNT(*) num_active_signatures, usuarios.cidade, usuarios.estado
    FROM {ASSINATURAS_TABLE_NAME} assinaturas
    INNER JOIN {USUARIOS_TABLE_NAME} usuarios ON assinaturas.codigo_usuario = usuarios.codigo_usuario
    WHERE assinaturas.data_cancelamento IS NULL
    GROUP BY usuarios.cidade, usuarios.estado
    """
)
# df_5.show()

# 6. Tarifações por região geográfica



import ipdb; ipdb.set_trace()
