"""Entrypoint for the data processing and visualization with Spark
NOTE: Replace the file names with the URLs when moving this to an actual Spark
Session in Azure
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType, FloatType
)


spark = SparkSession.builder.getOrCreate()

ASSINATURAS_FILE_NAME = "data/assinaturas.csv"
TRANSACOES_FILE_NAME = "data/transacoes.csv"
ASSINATURAS_TABLE_NAME = "assinaturas"
TRANSACOES_TABLE_NAME = "transacoes"

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

PRODUTO_FILE_NAME = "data/produto.csv"
PRODUTO_TABLE_NAME = "produto"

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

USUARIOS_FILE_NAME = "data/usuarios.csv"
USUARIOS_TABLE_NAME = "usuarios"

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

METODO_PAGAMENTO_FILE_NAME = "data/metodo_pagamento.csv"
METODO_PAGAMENTO_TABLE_NAME = "metodo_pagamento"

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

CONFIGURACAO_TARIFACAO_FILE_NAME = "data/configuracao_tarifacao.csv"
CONFIGURACAO_TARIFACAO_TABLE_NAME = "configuracao_tarifacao"

# configuracao_tarifacao data
# This data has a `valor_step` column with a comma (",") as decimal separator
# I coult not find a way for Pyspark to handle that, so the suggested approach
# is to first load it into a Pandas dataframe, save the handled data, and
# import it again
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
    CONFIGURACAO_TARIFACAO_FILE_NAME,
    sep=";",
    header=True,
    schema=configuracao_tarifacao_schema,
)
df_configuracao_tarifacao.createOrReplaceTempView(
    CONFIGURACAO_TARIFACAO_TABLE_NAME
)


import ipdb; ipdb.set_trace()
