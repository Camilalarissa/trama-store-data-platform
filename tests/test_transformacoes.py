from datetime import datetime
from decimal import Decimal

from pyspark.sql import functions as F

from trama.transformacoes import (
    COLUNAS_VENDAS,
    REGRAS_VENDAS,
    limpar_vendas_pdv,
    padronizar_clientes,
    padronizar_estoque,
    padronizar_lojas,
    padronizar_produtos,
    preparar_pedidos_ecommerce,
    separar_rejeitados,
    texto_para_decimal,
)


def test_texto_para_decimal_aceita_formato_brasileiro_e_americano(spark):
    df = spark.createDataFrame([("1.234,56",), ("59,90",), ("59.90",), (" 10 ",)], ["valor"])
    valores = [linha.v for linha in df.select(texto_para_decimal("valor").alias("v")).collect()]
    assert valores == [Decimal("1234.56"), Decimal("59.90"), Decimal("59.90"), Decimal("10.00")]


def test_pdv_remove_duplicatas_e_tipa_colunas(ler_bronze):
    bronze = ler_bronze("pdv_vendas")
    silver = limpar_vendas_pdv(bronze)
    assert silver.columns == COLUNAS_VENDAS
    assert bronze.count() > silver.count()  # o gerador manda linhas duplicadas
    assert silver.groupBy("id_venda", "id_item").count().filter("count > 1").count() == 0
    tipos = dict(silver.dtypes)
    assert tipos["preco_unitario"] == "decimal(12,2)"
    assert tipos["data_hora_venda"] == "timestamp_ntz"
    assert silver.filter("data_hora_venda IS NULL").count() == 0


def test_pdv_calcula_valor_liquido(spark):
    bronze = spark.createDataFrame(
        [("L001-1", "1", "L001", "01/10/2026 10:00:00", "", "sku1", "2", "59,90", "10,00", "Pix", "V1", "a.csv")],
        ["id_cupom", "item", "id_loja", "data_hora", "id_cliente", "sku", "quantidade",
         "preco_unitario", "desconto", "forma_pagamento", "id_vendedor", "_arquivo_origem"],
    )
    linha = limpar_vendas_pdv(bronze).first()
    assert linha.valor_bruto == Decimal("119.80")
    assert linha.valor_liquido == Decimal("109.80")
    assert linha.id_cliente is None  # vazio vira nulo
    assert linha.sku == "SKU1"
    assert linha.data_hora_venda == datetime(2026, 10, 1, 10, 0, 0)


def test_regras_de_qualidade_separam_linhas_invalidas(ler_bronze):
    silver = limpar_vendas_pdv(ler_bronze("pdv_vendas"))
    aprovados, rejeitados = separar_rejeitados(silver, REGRAS_VENDAS)
    assert aprovados.count() + rejeitados.count() == silver.count()
    assert rejeitados.count() > 0
    assert aprovados.filter("sku IS NULL OR quantidade <= 0").count() == 0
    motivos = {linha.motivo_rejeicao for linha in rejeitados.select("motivo_rejeicao").distinct().collect()}
    assert motivos & {"sku_ausente", "quantidade_invalida"}


def test_ecommerce_mantem_evento_mais_recente(spark):
    eventos = [
        ("PED-1", "app", "aprovado", "2026-10-01T03:00:00Z", "2026-10-01T03:00:00Z"),
        ("PED-1", "app", "cancelado", "2026-10-01T03:00:00Z", "2026-10-01T09:00:00Z"),
    ]
    df = spark.createDataFrame(
        [
            (p, c, s, cr, at, {"id_cliente": "C1"}, [{"sku": "sku1", "quantidade": "1", "preco_unitario": "99.9", "desconto": "0"}],
             {"metodo": "Pix"}, {"modalidade": "retira_loja", "id_loja_retirada": "L001"}, "x.json")
            for p, c, s, cr, at in eventos
        ],
        "pedido_id string, canal string, status string, criado_em string, atualizado_em string, "
        "cliente struct<id_cliente:string>, "
        "itens array<struct<sku:string,quantidade:string,preco_unitario:string,desconto:string>>, "
        "pagamento struct<metodo:string>, entrega struct<modalidade:string,id_loja_retirada:string>, "
        "_arquivo_origem string",
    )
    linhas = preparar_pedidos_ecommerce(df).collect()
    assert len(linhas) == 1
    assert linhas[0].status == "cancelado"
    assert linhas[0].id_loja_venda == "ECOM"
    assert linhas[0].id_loja_retirada == "L001"
    # 03:00 UTC = 00:00 em Brasília
    assert linhas[0].data_hora_venda == datetime(2026, 10, 1, 0, 0, 0)


def test_ecommerce_explode_itens_no_mesmo_esquema_do_pdv(ler_bronze):
    pdv = limpar_vendas_pdv(ler_bronze("pdv_vendas"))
    ecommerce = preparar_pedidos_ecommerce(ler_bronze("ecommerce_pedidos"))
    assert ecommerce.columns == COLUNAS_VENDAS
    assert ecommerce.groupBy("id_venda", "id_item").count().filter("count > 1").count() == 0
    vendas = pdv.unionByName(ecommerce)
    canais = {linha.canal for linha in vendas.select("canal").distinct().collect()}
    assert canais == {"loja_fisica", "app", "site"}


def test_clientes_sem_cpf_aberto_e_email_padronizado(ler_bronze):
    clientes = padronizar_clientes(ler_bronze("crm_clientes"))
    assert "cpf" not in clientes.columns
    assert clientes.filter(F.length("cpf_hash") != 64).count() == 0
    assert clientes.filter((F.col("email") != F.lower(F.trim("email")))).count() == 0
    assert clientes.groupBy("id_cliente").count().filter("count > 1").count() == 0


def test_cadastros_e_estoque_tipados(ler_bronze):
    produtos = padronizar_produtos(ler_bronze("erp_produtos"))
    assert produtos.groupBy("sku").count().filter("count > 1").count() == 0
    assert dict(produtos.dtypes)["preco_lista"] == "decimal(12,2)"
    lojas = padronizar_lojas(ler_bronze("erp_lojas"))
    assert lojas.count() == 32
    estoque = padronizar_estoque(ler_bronze("estoque"))
    assert dict(estoque.dtypes)["qtd_disponivel"] == "int"
    assert estoque.filter("qtd_disponivel = 0").count() > 0
