# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Silver: vendas omnichannel
# MAGIC
# MAGIC Junta as vendas das **lojas físicas (PDV)** e do **e-commerce (site e app)** em uma única
# MAGIC tabela, `silver.vendas_itens`, com uma linha por item vendido.
# MAGIC
# MAGIC | Tratamento | PDV | E-commerce |
# MAGIC |---|---|---|
# MAGIC | Formato | CSV, vírgula decimal, data `dd/MM/yyyy` | JSON com itens aninhados (explode) |
# MAGIC | Horário | Já em Brasília | UTC → Brasília |
# MAGIC | Duplicatas | Reenvio de linhas | Evento repetido; cancelamento posterior |
# MAGIC | Atualização | — | MERGE só aceita versão mais recente (`atualizado_em`) |
# MAGIC
# MAGIC Linhas que falham nas regras de qualidade vão para `silver.vendas_rejeitadas`, com o motivo.

# COMMAND ----------

dbutils.widgets.text("catalogo", "trama_store_dev", "Catálogo")

# COMMAND ----------

# MAGIC %run ./_comum

# COMMAND ----------

from trama.incremental import processar_incremental
from trama.transformacoes import REGRAS_VENDAS, limpar_vendas_pdv, preparar_pedidos_ecommerce

# As duas fontes escrevem na mesma tabela: rodam uma depois da outra para evitar
# conflito de escrita concorrente no MERGE.
for origem, transformar in [("pdv_vendas", limpar_vendas_pdv), ("ecommerce_pedidos", preparar_pedidos_ecommerce)]:
    print(f"bronze.{origem} → silver.vendas_itens")
    processar_incremental(
        spark,
        origem=f"{CATALOGO}.bronze.{origem}",
        destino=f"{CATALOGO}.silver.vendas_itens",
        checkpoint=f"{CHECKPOINTS}/silver_vendas_{origem}",
        transformar=transformar,
        chaves=["id_venda", "id_item"],
        coluna_versao="atualizado_em",
        regras=REGRAS_VENDAS,
        destino_rejeitados=f"{CATALOGO}.silver.vendas_rejeitadas",
        agrupar_por=["data_venda", "canal"],  # colunas mais filtradas (Liquid Clustering)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência
# MAGIC O Liquid Clustering por `data_venda` e `canal` é definido na criação da tabela
# MAGIC (`agrupar_por`). Sem `OPTIMIZE` manual: a predictive optimization cuida disso (D13).

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT canal, status, COUNT(*) AS itens, SUM(valor_liquido) AS valor_liquido
        FROM {CATALOGO}.silver.vendas_itens
        GROUP BY ALL ORDER BY canal, status
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT motivo_rejeicao, COUNT(*) AS linhas
        FROM {CATALOGO}.silver.vendas_rejeitadas
        GROUP BY ALL ORDER BY linhas DESC
    """)
)
