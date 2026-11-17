# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Silver: estoque diário
# MAGIC
# MAGIC Foto do estoque no fim do dia, por local (loja ou centro de distribuição) e SKU.
# MAGIC Chave: `data_estoque + id_local + sku`, o que torna a carga idempotente.

# COMMAND ----------

dbutils.widgets.text("catalogo", "trama_store_dev", "Catálogo")

# COMMAND ----------

# MAGIC %run ./_comum

# COMMAND ----------

from trama.incremental import processar_incremental
from trama.transformacoes import padronizar_estoque

processar_incremental(
    spark,
    origem=f"{CATALOGO}.bronze.estoque",
    destino=f"{CATALOGO}.silver.estoque",
    checkpoint=f"{CHECKPOINTS}/silver_estoque",
    transformar=padronizar_estoque,
    chaves=["data_estoque", "id_local", "sku"],
    agrupar_por=["data_estoque", "id_local"],  # Liquid Clustering definido na criação (D13)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT data_estoque, COUNT(*) AS posicoes, SUM(CASE WHEN qtd_disponivel = 0 THEN 1 ELSE 0 END) AS zerados
        FROM {CATALOGO}.silver.estoque
        GROUP BY ALL ORDER BY data_estoque DESC LIMIT 10
    """)
)
