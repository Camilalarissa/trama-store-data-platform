"""Carga incremental e idempotente da Bronze para a Silver.

Padrão usado: Structured Streaming lendo a tabela Bronze + `foreachBatch` com
`MERGE INTO` na Silver, com `trigger(availableNow=True)`:

- Incremental: o checkpoint guarda até onde a Bronze já foi lida;
  cada execução processa só as linhas novas.
- Idempotente: o MERGE usa a chave de negócio, então reprocessar um lote
  não duplica dados.
- Atualizações tardias: com `coluna_versao`, uma linha só é sobrescrita por
  outra mais recente (ex.: o cancelamento de um pedido que chega horas depois).
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from trama.transformacoes import separar_rejeitados


def montar_merge_sql(
    destino: str,
    origem: str,
    chaves: list[str],
    coluna_versao: str | None = None,
    somente_inserir: bool = False,
) -> str:
    """Monta o comando MERGE (função pura, testada com pytest)."""
    condicao = " AND ".join(f"t.{chave} <=> s.{chave}" for chave in chaves)
    linhas = [f"MERGE WITH SCHEMA EVOLUTION INTO {destino} AS t", f"USING {origem} AS s", f"ON {condicao}"]
    if not somente_inserir:
        if coluna_versao:
            linhas.append(f"WHEN MATCHED AND s.{coluna_versao} > t.{coluna_versao} THEN UPDATE SET *")
        else:
            linhas.append("WHEN MATCHED THEN UPDATE SET *")
    linhas.append("WHEN NOT MATCHED THEN INSERT *")
    return "\n".join(linhas)


def _merge(
    lote: DataFrame,
    destino: str,
    chaves: list[str],
    coluna_versao: str | None,
    somente_inserir: bool,
    agrupar_por: list[str] | None = None,
) -> None:
    sessao = lote.sparkSession  # no foreachBatch, use a sessão do próprio lote
    # Cria a tabela vazia na 1ª execução, já com o Liquid Clustering definido (D13).
    # Nas execuções seguintes, o modo "ignore" não faz nada.
    escritor = lote.limit(0).write.mode("ignore")
    if agrupar_por:
        escritor = escritor.clusterBy(*agrupar_por)
    escritor.saveAsTable(destino)
    visao = "lote_" + destino.replace(".", "_")
    lote.createOrReplaceTempView(visao)
    sessao.sql(montar_merge_sql(destino, visao, chaves, coluna_versao, somente_inserir))


def processar_incremental(
    spark,
    origem: str,
    destino: str,
    checkpoint: str,
    transformar: Callable[[DataFrame], DataFrame],
    chaves: list[str],
    coluna_versao: str | None = None,
    regras: dict[str, str] | None = None,
    destino_rejeitados: str | None = None,
    agrupar_por: list[str] | None = None,
) -> None:
    """Lê as linhas novas da tabela `origem`, transforma e faz MERGE em `destino`.

    Se `regras` for informado, as linhas reprovadas vão para `destino_rejeitados`
    (quarentena) com o motivo, em vez de entrarem na Silver.

    `agrupar_por` define as colunas do Liquid Clustering na criação da tabela `destino`.
    """

    def processar_lote(lote: DataFrame, lote_id: int) -> None:
        dados = transformar(lote)
        if regras:
            dados, rejeitados = separar_rejeitados(dados, regras)
            rejeitados = rejeitados.withColumn("lote_id", F.lit(lote_id)).withColumn(
                "processado_em", F.current_timestamp()
            )
            _merge(rejeitados, destino_rejeitados, chaves, None, somente_inserir=True)
        _merge(dados, destino, chaves, coluna_versao, somente_inserir=False, agrupar_por=agrupar_por)

    consulta = (
        spark.readStream.table(origem)
        .writeStream.foreachBatch(processar_lote)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    consulta.awaitTermination()
