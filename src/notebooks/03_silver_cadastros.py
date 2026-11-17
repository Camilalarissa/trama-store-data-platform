# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: cadastros (lojas, produtos e clientes)
# MAGIC
# MAGIC Cada cadastro é atualizado com **MERGE** pela chave de negócio (SCD tipo 1: guarda
# MAGIC a versão mais recente). Só as linhas novas da Bronze são lidas em cada execução.
# MAGIC
# MAGIC **LGPD:** o CPF é transformado em hash (`cpf_hash`) e não é gravado aberto na Silver.

# COMMAND ----------

dbutils.widgets.text("catalogo", "trama_store_dev", "Catálogo")

# COMMAND ----------

# MAGIC %run ./_comum

# COMMAND ----------

from trama.incremental import processar_incremental
from trama.transformacoes import padronizar_clientes, padronizar_lojas, padronizar_produtos

CADASTROS = [
    # (bronze, silver, transformação, chave, coluna de versão)
    ("erp_lojas", "lojas", padronizar_lojas, ["id_loja"], None),
    ("erp_produtos", "produtos", padronizar_produtos, ["sku"], "atualizado_em"),
    ("crm_clientes", "clientes", padronizar_clientes, ["id_cliente"], "atualizado_em"),
]

for origem, destino, transformar, chaves, versao in CADASTROS:
    print(f"bronze.{origem} → silver.{destino}")
    processar_incremental(
        spark,
        origem=f"{CATALOGO}.bronze.{origem}",
        destino=f"{CATALOGO}.silver.{destino}",
        checkpoint=f"{CHECKPOINTS}/silver_{destino}",
        transformar=transformar,
        chaves=chaves,
        coluna_versao=versao,
    )

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {CATALOGO}.silver.clientes IS 'Clientes do CRM. CPF armazenado apenas como hash SHA-256.'")
display(spark.sql(f"SELECT * FROM {CATALOGO}.silver.clientes LIMIT 10"))
