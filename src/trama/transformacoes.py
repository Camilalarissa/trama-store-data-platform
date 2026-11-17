"""Transformações da camada Silver (Bronze -> Silver).

Cada função recebe um DataFrame com o formato da Bronze e devolve um DataFrame
limpo e tipado. As funções são puras (não leem nem gravam tabelas), por isso
podem ser testadas com pytest em um Spark local e reaproveitadas nos notebooks.

Convenção de horário: todas as datas/horas da Silver são TIMESTAMP_NTZ
(sem fuso) no horário de Brasília. O e-commerce manda UTC e é convertido.
Assim o valor não muda conforme o fuso da sessão ou da máquina de quem lê.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

FUSO_BRASIL = "America/Sao_Paulo"
DINHEIRO = "decimal(12,2)"
ID_ECOMMERCE = "ECOM"

# Esquema único de vendas: loja física e e-commerce na mesma tabela (omnichannel)
COLUNAS_VENDAS = [
    "id_venda",
    "id_item",
    "canal",
    "id_loja_venda",
    "id_loja_retirada",
    "modalidade_entrega",
    "id_cliente",
    "sku",
    "quantidade",
    "preco_unitario",
    "valor_bruto",
    "desconto",
    "valor_liquido",
    "forma_pagamento",
    "status",
    "data_hora_venda",
    "data_venda",
    "atualizado_em",
    "arquivo_origem",
]

# Regras de qualidade: nome da regra -> condição que REPROVA a linha
REGRAS_VENDAS = {
    "sku_ausente": "sku IS NULL",
    "quantidade_invalida": "quantidade IS NULL OR quantidade <= 0",
    "preco_invalido": "preco_unitario IS NULL OR preco_unitario <= 0",
    "data_invalida": "data_hora_venda IS NULL",
    "desconto_maior_que_valor": "desconto > valor_bruto",
}


# --------------------------------------------------------------------------
# Funções auxiliares
# --------------------------------------------------------------------------


def texto_para_decimal(coluna: str | Column) -> Column:
    """Converte números em texto para decimal, aceitando os dois formatos.

    '1.234,56' (padrão brasileiro) -> 1234.56
    '59.90'    (padrão americano)  -> 59.90
    """
    texto = F.trim(F.col(coluna) if isinstance(coluna, str) else coluna)
    padrao_br = F.regexp_replace(F.regexp_replace(texto, r"\.", ""), ",", ".")
    return F.when(texto.contains(","), padrao_br).otherwise(texto).cast(DINHEIRO)


def utc_para_ntz(coluna: str) -> Column:
    """'2026-10-01T03:00:00Z' -> 2026-10-01 03:00:00 (UTC, sem fuso)."""
    return F.to_timestamp_ntz(F.col(coluna), F.lit("yyyy-MM-dd'T'HH:mm:ss'Z'"))


def para_horario_brasilia(coluna_utc: Column) -> Column:
    """Converte um TIMESTAMP_NTZ em UTC para o horário de Brasília (também NTZ)."""
    return F.convert_timezone(F.lit("UTC"), F.lit(FUSO_BRASIL), coluna_utc)


def vazio_para_nulo(coluna: str) -> Column:
    texto = F.trim(F.col(coluna).cast("string"))
    return F.when(texto == "", None).otherwise(texto)


def manter_mais_recente(df: DataFrame, chaves: list[str], ordem: str) -> DataFrame:
    """Deduplica mantendo só a versão mais recente de cada chave."""
    janela = Window.partitionBy(*chaves).orderBy(F.col(ordem).desc())
    return df.withColumn("_ordem", F.row_number().over(janela)).filter("_ordem = 1").drop("_ordem")


def separar_rejeitados(df: DataFrame, regras: dict[str, str]) -> tuple[DataFrame, DataFrame]:
    """Aplica regras de qualidade e separa as linhas aprovadas das reprovadas.

    As reprovadas ganham a coluna `motivo_rejeicao` (ex.: 'sku_ausente, preco_invalido')
    e vão para uma tabela de quarentena, em vez de serem descartadas em silêncio.
    """
    motivo = F.concat_ws(", ", *[F.when(F.expr(condicao), F.lit(nome)) for nome, condicao in regras.items()])
    marcado = df.withColumn("motivo_rejeicao", motivo)
    aprovados = marcado.filter(F.col("motivo_rejeicao") == "").drop("motivo_rejeicao")
    rejeitados = marcado.filter(F.col("motivo_rejeicao") != "")
    return aprovados, rejeitados


def _completar_valores(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("valor_bruto", (F.col("quantidade") * F.col("preco_unitario")).cast(DINHEIRO))
        .withColumn("valor_liquido", (F.col("valor_bruto") - F.col("desconto")).cast(DINHEIRO))
        .withColumn("data_venda", F.to_date("data_hora_venda"))
    )


# --------------------------------------------------------------------------
# Vendas
# --------------------------------------------------------------------------


def limpar_vendas_pdv(df: DataFrame) -> DataFrame:
    """PDV (lojas físicas): CSV com tudo em texto, vírgula decimal e data dd/MM/yyyy."""
    padronizado = df.select(
        F.trim("id_cupom").alias("id_venda"),
        F.col("item").cast("int").alias("id_item"),
        F.lit("loja_fisica").alias("canal"),
        F.trim("id_loja").alias("id_loja_venda"),
        F.lit(None).cast("string").alias("id_loja_retirada"),
        F.lit("presencial").alias("modalidade_entrega"),
        vazio_para_nulo("id_cliente").alias("id_cliente"),
        F.upper(vazio_para_nulo("sku")).alias("sku"),
        F.col("quantidade").cast("int").alias("quantidade"),
        texto_para_decimal("preco_unitario").alias("preco_unitario"),
        F.coalesce(texto_para_decimal("desconto"), F.lit(0).cast(DINHEIRO)).alias("desconto"),
        F.trim("forma_pagamento").alias("forma_pagamento"),
        F.lit("concluida").alias("status"),
        F.to_timestamp_ntz(F.col("data_hora"), F.lit("dd/MM/yyyy HH:mm:ss")).alias("data_hora_venda"),
        F.col("_arquivo_origem").alias("arquivo_origem"),
    )
    return (
        _completar_valores(padronizado)
        .withColumn("atualizado_em", F.col("data_hora_venda"))
        .dropDuplicates(["id_venda", "id_item"])  # reenvios do PDV
        .select(*COLUNAS_VENDAS)
    )


def preparar_pedidos_ecommerce(df: DataFrame) -> DataFrame:
    """E-commerce: um evento JSON por pedido, com itens aninhados e horário em UTC.

    1. Mantém só o evento mais recente de cada pedido (ex.: o cancelamento).
    2. Explode o array de itens: uma linha por item, como no PDV.
    3. Converte UTC para o horário de Brasília.
    """
    eventos = df.withColumn("_atualizado_utc", utc_para_ntz("atualizado_em"))
    pedidos = manter_mais_recente(eventos, ["pedido_id"], "_atualizado_utc")
    itens = pedidos.select("*", F.posexplode("itens").alias("_posicao", "_item"))
    padronizado = itens.select(
        F.trim("pedido_id").alias("id_venda"),
        (F.col("_posicao") + 1).cast("int").alias("id_item"),
        F.lower(F.trim("canal")).alias("canal"),
        F.lit(ID_ECOMMERCE).alias("id_loja_venda"),
        F.col("entrega.id_loja_retirada").alias("id_loja_retirada"),
        F.col("entrega.modalidade").alias("modalidade_entrega"),
        F.col("cliente.id_cliente").alias("id_cliente"),
        F.upper(F.trim("_item.sku")).alias("sku"),
        F.col("_item.quantidade").cast("int").alias("quantidade"),
        F.col("_item.preco_unitario").cast(DINHEIRO).alias("preco_unitario"),
        F.coalesce(F.col("_item.desconto").cast(DINHEIRO), F.lit(0).cast(DINHEIRO)).alias("desconto"),
        F.col("pagamento.metodo").alias("forma_pagamento"),
        F.col("status"),
        para_horario_brasilia(utc_para_ntz("criado_em")).alias("data_hora_venda"),
        para_horario_brasilia(F.col("_atualizado_utc")).alias("atualizado_em"),
        F.col("_arquivo_origem").alias("arquivo_origem"),
    )
    return _completar_valores(padronizado).select(*COLUNAS_VENDAS)


# --------------------------------------------------------------------------
# Cadastros e estoque
# --------------------------------------------------------------------------


def padronizar_clientes(df: DataFrame) -> DataFrame:
    """CRM: e-mail e nome padronizados; CPF vira hash (LGPD: a Silver não guarda o CPF)."""
    com_data = df.withColumn("atualizado_em", F.to_timestamp_ntz(F.col("atualizado_em")))
    recentes = manter_mais_recente(com_data, ["id_cliente"], "atualizado_em")
    cpf_somente_digitos = F.regexp_replace("cpf", r"\D", "")
    return recentes.select(
        F.trim("id_cliente").alias("id_cliente"),
        F.initcap(F.lower(F.trim("nome"))).alias("nome"),
        F.sha2(cpf_somente_digitos, 256).alias("cpf_hash"),
        F.lower(F.trim("email")).alias("email"),
        F.to_date("data_nascimento").alias("data_nascimento"),
        F.col("endereco.cidade").alias("cidade"),
        F.upper(F.col("endereco.uf")).alias("uf"),
        F.col("programa_fidelidade.cartao_trama").cast("boolean").alias("cartao_trama"),
        F.col("consentimentos.aceita_marketing").cast("boolean").alias("aceita_marketing"),
        F.to_date("data_cadastro").alias("data_cadastro"),
        F.col("atualizado_em"),
    )


def padronizar_produtos(df: DataFrame) -> DataFrame:
    com_data = df.withColumn("atualizado_em", F.to_timestamp_ntz(F.col("atualizado_em")))
    recentes = manter_mais_recente(com_data, ["sku"], "atualizado_em")
    return recentes.select(
        F.upper(F.trim("sku")).alias("sku"),
        F.trim("descricao").alias("descricao"),
        F.trim("departamento").alias("departamento"),
        F.trim("categoria").alias("categoria"),
        F.trim("cor").alias("cor"),
        F.trim("tamanho").alias("tamanho"),
        texto_para_decimal("preco_lista").alias("preco_lista"),
        F.col("atualizado_em"),
    )


def padronizar_lojas(df: DataFrame) -> DataFrame:
    return df.select(
        F.trim("id_loja").alias("id_loja"),
        F.trim("nome").alias("nome"),
        F.trim("tipo").alias("tipo"),
        F.trim("cidade").alias("cidade"),
        F.upper(F.trim("uf")).alias("uf"),
        F.trim("regiao").alias("regiao"),
        F.col("area_m2").cast("int").alias("area_m2"),
        F.to_date("data_inauguracao").alias("data_inauguracao"),
    ).dropDuplicates(["id_loja"])


def padronizar_estoque(df: DataFrame) -> DataFrame:
    return df.select(
        F.to_date("data_estoque").alias("data_estoque"),
        F.trim("id_local").alias("id_local"),
        F.upper(F.trim("sku")).alias("sku"),
        F.col("qtd_disponivel").cast("int").alias("qtd_disponivel"),
    ).dropDuplicates(["data_estoque", "id_local", "sku"])
