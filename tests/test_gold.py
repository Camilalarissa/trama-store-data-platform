"""Executa o notebook 06_gold.sql em um Spark local para validar o SQL.

Os comandos exclusivos do Databricks (widgets, USE CATALOG, CLUSTER BY e
CREATE OR REPLACE em tabelas não-Delta) são adaptados antes da execução.
"""

import re
from pathlib import Path

import pytest

from trama.transformacoes import (
    REGRAS_VENDAS,
    limpar_vendas_pdv,
    padronizar_clientes,
    padronizar_estoque,
    padronizar_lojas,
    padronizar_produtos,
    preparar_pedidos_ecommerce,
    separar_rejeitados,
)

NOTEBOOK_GOLD = Path(__file__).resolve().parents[1] / "src" / "notebooks" / "06_gold.sql"


def _comandos_gold() -> list[str]:
    celulas = NOTEBOOK_GOLD.read_text(encoding="utf-8").split("-- COMMAND ----------")
    comandos = []
    for celula in celulas:
        sql = "\n".join(linha for linha in celula.splitlines() if not linha.startswith("-- ")).strip()
        if not sql or sql.startswith(("CREATE WIDGET", "USE CATALOG")):
            continue
        sql = sql.rstrip(";")
        sql = re.sub(r"CLUSTER BY \([^)]*\)\n", "", sql)
        sql = sql.replace("CREATE OR REPLACE TABLE", "CREATE TABLE")
        comandos.append(sql)
    return comandos


@pytest.fixture(scope="module")
def gold(spark, ler_bronze):
    for schema in ("silver", "gold"):
        spark.sql(f"DROP DATABASE IF EXISTS {schema} CASCADE")
        spark.sql(f"CREATE DATABASE {schema}")

    vendas = limpar_vendas_pdv(ler_bronze("pdv_vendas")).unionByName(
        preparar_pedidos_ecommerce(ler_bronze("ecommerce_pedidos"))
    )
    aprovadas, rejeitadas = separar_rejeitados(vendas, REGRAS_VENDAS)
    tabelas = {
        "lojas": padronizar_lojas(ler_bronze("erp_lojas")),
        "produtos": padronizar_produtos(ler_bronze("erp_produtos")),
        "clientes": padronizar_clientes(ler_bronze("crm_clientes")),
        "estoque": padronizar_estoque(ler_bronze("estoque")),
        "vendas_itens": aprovadas,
        "vendas_rejeitadas": rejeitadas.selectExpr("*", "current_timestamp() AS processado_em"),
    }
    for nome, df in tabelas.items():
        df.write.saveAsTable(f"silver.{nome}")

    for comando in _comandos_gold():
        spark.sql(comando)
    return spark


def test_todas_as_tabelas_gold_tem_dados(gold):
    nomes = [linha.tableName for linha in gold.sql("SHOW TABLES IN gold").collect()]
    assert len(nomes) == 10
    for nome in nomes:
        assert gold.table(f"gold.{nome}").count() > 0, nome


def test_fato_exclui_pedidos_cancelados(gold):
    assert gold.sql("SELECT COUNT(*) AS n FROM silver.vendas_itens WHERE status = 'cancelado'").first().n > 0
    canceladas = gold.sql("""
        SELECT COUNT(*) AS n FROM gold.fato_vendas f
        JOIN silver.vendas_itens v USING (id_venda, id_item)
        WHERE v.status = 'cancelado'
    """).first().n
    assert canceladas == 0


def test_faturamento_do_kpi_bate_com_a_fato(gold):
    kpi = gold.sql("SELECT SUM(faturamento_liquido) AS v FROM gold.kpi_vendas_diarias").first().v
    fato = gold.sql("SELECT SUM(valor_liquido) AS v FROM gold.fato_vendas").first().v
    assert kpi == fato


def test_perfis_de_cliente(gold):
    perfis = {linha.perfil for linha in gold.table("gold.kpi_clientes_perfil").collect()}
    assert perfis == {"Omnichannel", "Só loja física", "Só digital"}


def test_dim_cliente_sem_dados_pessoais(gold):
    colunas = set(gold.table("gold.dim_cliente").columns)
    assert not colunas & {"nome", "email", "cpf", "cpf_hash", "data_nascimento"}

