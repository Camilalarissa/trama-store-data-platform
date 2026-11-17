import sys
import tempfile
from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trama.fontes import FONTES  # noqa: E402
from trama.gerador import gerar_periodo  # noqa: E402

INICIO = date(2026, 10, 1)
FIM = date(2026, 10, 2)


@pytest.fixture(scope="session")
def spark():
    sessao = (
        SparkSession.builder.master("local[2]")
        .appName("testes-trama")
        .config("spark.sql.session.timeZone", "UTC")  # com TIMESTAMP_NTZ, o resultado não depende deste fuso
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp(prefix="warehouse-"))
        .getOrCreate()
    )
    yield sessao
    sessao.stop()


@pytest.fixture(scope="session")
def landing(tmp_path_factory):
    pasta = tmp_path_factory.mktemp("landing")
    gerar_periodo(INICIO, FIM, pasta)
    return pasta


@pytest.fixture(scope="session")
def ler_bronze(spark, landing):
    """Lê uma fonte como a Bronze leria: tudo em texto + caminho do arquivo de origem.

    No JSON, o Auto Loader (sem `cloudFiles.inferColumnTypes`) devolve TODA coluna de
    primeiro nível como STRING: um objeto ou array aninhado chega como o texto JSON dele.
    `primitivesAsString` não serve para imitar isso, porque mantém as structs e arrays.
    Por isso o teste descobre os nomes das colunas e lê de novo com um schema só de STRING.
    """

    def _ler(fonte: str):
        config = FONTES[fonte]
        caminho = str(landing / fonte)
        leitor = spark.read.format(config["formato"]).options(**config["opcoes"])
        if config["formato"] == "json":
            colunas = spark.read.json(caminho).columns
            leitor = leitor.schema(", ".join(f"`{coluna}` STRING" for coluna in colunas))
        if fonte == "pdv_vendas":
            leitor = leitor.option("recursiveFileLookup", "true")
        df = leitor.load(caminho)
        return df.withColumn("_arquivo_origem", F.col("_metadata.file_path"))

    return _ler
