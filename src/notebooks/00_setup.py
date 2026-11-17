# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do Unity Catalog
# MAGIC
# MAGIC Cria a estrutura do projeto. Pode rodar quantas vezes quiser: tudo usa `IF NOT EXISTS`.
# MAGIC
# MAGIC ```
# MAGIC <catalogo>
# MAGIC ├── bronze   → tabelas brutas + volumes landing (arquivos) e checkpoints (estado dos streams)
# MAGIC ├── silver   → tabelas limpas, tipadas e deduplicadas
# MAGIC └── gold     → modelo dimensional e KPIs para o negócio
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalogo", "trama_store_dev", "Catálogo")
CATALOGO = dbutils.widgets.get("catalogo")

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
except Exception as erro:  # sem permissão para criar catálogos
    raise RuntimeError(
        f"Não foi possível criar o catálogo '{CATALOGO}'. Rode o projeto com o parâmetro "
        "catalogo=workspace (catálogo padrão do Databricks Free Edition) ou peça a um admin "
        "para criar o catálogo."
    ) from erro

for schema, descricao in [
    ("bronze", "Dados brutos, como chegaram dos sistemas-fonte"),
    ("silver", "Dados limpos, tipados, deduplicados e validados"),
    ("gold", "Modelo dimensional e indicadores para o negócio"),
]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema} COMMENT '{descricao}'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronze.landing COMMENT 'Arquivos recebidos dos sistemas-fonte'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronze.checkpoints COMMENT 'Checkpoints e schemas do Auto Loader e dos streams'")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOGO}"))
