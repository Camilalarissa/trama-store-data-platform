# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Gerar dados dos sistemas-fonte
# MAGIC
# MAGIC Simula a chegada dos arquivos do dia na landing (`/Volumes/<catalogo>/bronze/landing`).
# MAGIC Em uma empresa real, esta etapa não existiria: os arquivos viriam do ERP, do PDV,
# MAGIC do CRM, do e-commerce e do WMS.
# MAGIC
# MAGIC | Parâmetro | Uso |
# MAGIC |---|---|
# MAGIC | `data_ref` | Dia a gerar (`AAAA-MM-DD`). Vazio = hoje no horário de Brasília. |
# MAGIC | `dias_historico` | `0` = só o `data_ref`. `30` = carga histórica dos 30 dias até o `data_ref` (só na primeira vez). |

# COMMAND ----------

dbutils.widgets.text("catalogo", "trama_store_dev", "Catálogo")
dbutils.widgets.text("data_ref", "", "Data de referência (AAAA-MM-DD)")
dbutils.widgets.text("dias_historico", "0", "Dias de histórico")

# COMMAND ----------

# MAGIC %run ./_comum

# COMMAND ----------

from datetime import date, timedelta

from trama.gerador import gerar_dia, gerar_periodo, hoje_brasilia, ja_tem_carga_inicial

texto_data = dbutils.widgets.get("data_ref").strip()
data_ref = date.fromisoformat(texto_data) if texto_data else hoje_brasilia()
dias_historico = int(dbutils.widgets.get("dias_historico") or 0)

if dias_historico > 0:
    # Rodar o histórico de novo regravaria dias já ingeridos com conteúdo diferente e o mesmo
    # nome de arquivo. O Auto Loader ignora arquivo sobrescrito, então a Bronze ficaria
    # diferente da landing sem nenhum aviso. Melhor parar aqui.
    if ja_tem_carga_inicial(LANDING):
        raise RuntimeError(
            "A landing já tem a carga histórica. Use dias_historico=0 para gerar só o dia; "
            "para recomeçar do zero, apague os volumes landing e checkpoints e as tabelas."
        )
    inicio = data_ref - timedelta(days=dias_historico - 1)
    print(f"Carga histórica de {inicio} até {data_ref}")
    contagens = gerar_periodo(inicio, data_ref, LANDING)
else:
    # Na primeira execução, os cadastros (lojas, produtos, clientes) vêm completos
    primeira_vez = not ja_tem_carga_inicial(LANDING)
    print(f"Gerando {data_ref} (carga inicial: {primeira_vez})")
    contagens = gerar_dia(data_ref, LANDING, carga_inicial=primeira_vez)

display(spark.createDataFrame(sorted(contagens.items()), "fonte string, registros long"))

# Disponibiliza o total para as próximas tasks do Job
dbutils.jobs.taskValues.set(key="registros_gerados", value=sum(contagens.values()))
