# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração comum
# MAGIC
# MAGIC Executado no início de cada notebook com `%run ./_comum`. Como o `%run` roda no
# MAGIC **mesmo contexto**, as variáveis definidas aqui (`CATALOGO`, `LANDING`, ...) ficam
# MAGIC disponíveis no notebook que o chamou.
# MAGIC
# MAGIC Pré-requisito: o notebook chamador já criou o widget `catalogo`.

# COMMAND ----------

import os
import sys

# Permite importar o pacote `trama` (pasta src/trama) a partir de src/notebooks
PASTA_SRC = os.path.abspath("..")
if PASTA_SRC not in sys.path:
    sys.path.insert(0, PASTA_SRC)

import trama  # noqa: E402

# No compute serverless, o código do foreachBatch roda no servidor, onde a pasta
# do projeto não está instalada. Registrar o pacote "por valor" faz o código
# das funções viajar junto, em vez de ser importado pelo nome.
try:
    from pyspark import cloudpickle

    cloudpickle.register_pickle_by_value(trama)
except (ImportError, AttributeError) as erro:
    print(f"Aviso: não foi possível registrar o pacote trama no cloudpickle ({erro}).")

# COMMAND ----------

CATALOGO = dbutils.widgets.get("catalogo")  # noqa: F821 (dbutils existe no Databricks)
LANDING = f"/Volumes/{CATALOGO}/bronze/landing"
CHECKPOINTS = f"/Volumes/{CATALOGO}/bronze/checkpoints"

print(f"Catálogo: {CATALOGO}")
print(f"Landing: {LANDING}")
