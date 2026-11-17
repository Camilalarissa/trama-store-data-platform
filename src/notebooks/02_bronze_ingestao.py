# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: ingestão incremental com Auto Loader
# MAGIC
# MAGIC Para cada fonte, o Auto Loader (`cloudFiles`) lê **só os arquivos novos** da landing e
# MAGIC acrescenta na tabela Bronze correspondente.
# MAGIC
# MAGIC - **Tudo como texto:** a Bronze guarda o dado como chegou; a tipagem é feita na Silver.
# MAGIC - **Linhagem:** cada linha recebe o arquivo de origem e a data/hora da ingestão.
# MAGIC - **Evolução de schema:** colunas novas na fonte são adicionadas à tabela (`addNewColumns`).
# MAGIC   Quando isso acontece, o stream para de propósito para registrar o novo schema;
# MAGIC   a nova tentativa automática do Job continua a carga.
# MAGIC - **`availableNow`:** processa tudo o que está pendente e encerra (modo lote, ideal para Jobs).

# COMMAND ----------

dbutils.widgets.text("catalogo", "trama_store_dev", "Catálogo")

# COMMAND ----------

# MAGIC %run ./_comum

# COMMAND ----------

from pyspark.sql import functions as F

from trama.fontes import FONTES


def ingerir_fonte(fonte: str, config: dict) -> None:
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", config["formato"])
        .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/bronze_{fonte}/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .options(**config["opcoes"])
    )
    df = (
        leitor.load(f"{LANDING}/{fonte}")
        .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("_ingerido_em", F.current_timestamp())
    )
    consulta = (
        df.writeStream.option("checkpointLocation", f"{CHECKPOINTS}/bronze_{fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{CATALOGO}.bronze.{fonte}")
    )
    consulta.awaitTermination()


for fonte, config in FONTES.items():
    print(f"Ingerindo {fonte}...")
    ingerir_fonte(fonte, config)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência: linhas por tabela e por arquivo mais recente

# COMMAND ----------

resumo = [
    (fonte, spark.table(f"{CATALOGO}.bronze.{fonte}").count())
    for fonte in FONTES
]
display(spark.createDataFrame(resumo, "tabela string, linhas long"))
