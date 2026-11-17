import csv
import json
from datetime import date

from trama.gerador import LOJAS, PRODUTOS, gerar_dia


def test_mesma_data_gera_os_mesmos_arquivos(tmp_path):
    gerar_dia(date(2026, 10, 1), tmp_path / "a", carga_inicial=True)
    gerar_dia(date(2026, 10, 1), tmp_path / "b", carga_inicial=True)
    arquivo = "ecommerce_pedidos/pedidos_20261001.json"
    assert (tmp_path / "a" / arquivo).read_text() == (tmp_path / "b" / arquivo).read_text()


def test_carga_inicial_traz_todos_os_cadastros(tmp_path):
    contagens = gerar_dia(date(2026, 10, 1), tmp_path, carga_inicial=True)
    assert contagens["erp_lojas"] == len(LOJAS) == 32
    assert contagens["erp_produtos"] == len(PRODUTOS)
    assert contagens["crm_clientes"] > 5000


def test_dia_comum_traz_so_alteracoes(tmp_path):
    contagens = gerar_dia(date(2026, 10, 2), tmp_path)
    assert "erp_lojas" not in contagens
    assert contagens["erp_produtos"] == 6
    assert contagens["crm_clientes"] < 100


def test_pdv_usa_formato_brasileiro(tmp_path):
    gerar_dia(date(2026, 10, 2), tmp_path)
    with (tmp_path / "pdv_vendas" / "2026-10-02" / "pdv_L001.csv").open(encoding="utf-8") as arquivo:
        linha = next(csv.DictReader(arquivo, delimiter=";"))
    assert "," in linha["preco_unitario"]
    assert linha["data_hora"][2] == "/"


def test_ecommerce_tem_cancelamentos_e_horario_utc(tmp_path):
    gerar_dia(date(2026, 10, 2), tmp_path)
    linhas = (tmp_path / "ecommerce_pedidos" / "pedidos_20261002.json").read_text(encoding="utf-8").splitlines()
    eventos = [json.loads(linha) for linha in linhas]
    assert any(evento["status"] == "cancelado" for evento in eventos)
    assert all(evento["criado_em"].endswith("Z") for evento in eventos)
