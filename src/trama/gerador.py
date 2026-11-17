"""Gerador de dados sintéticos da Trama Store (varejo de moda omnichannel fictício).

Simula os arquivos que cada sistema-fonte entregaria por dia na área de landing:

| Sistema    | Pasta               | Formato                     | Conteúdo                              |
|------------|---------------------|-----------------------------|---------------------------------------|
| ERP        | erp_lojas/          | CSV (;)                     | Cadastro de lojas e CDs (carga inicial)|
| ERP        | erp_produtos/       | CSV (;) com vírgula decimal | Produtos novos ou alterados no dia     |
| CRM        | crm_clientes/       | JSON Lines (aninhado)       | Clientes novos ou alterados no dia     |
| PDV        | pdv_vendas/<data>/  | CSV (;) por loja            | Itens vendidos nas lojas físicas       |
| E-commerce | ecommerce_pedidos/  | JSON Lines (aninhado)       | Eventos de pedidos do site e do app    |
| WMS        | estoque/            | CSV (;)                     | Foto diária do estoque por local       |

Os dados vêm com "sujeira" proposital, como em sistemas reais: vírgula decimal,
linhas duplicadas, SKU vazio, quantidade inválida, e-mails despadronizados,
CPF com e sem máscara, pedidos reenviados e cancelamentos que chegam depois.

Tudo é determinístico: a mesma data sempre gera os mesmos arquivos, então
rodar o gerador de novo para um dia já gerado não muda nada (idempotência).
Nomes, CPFs e e-mails são inventados (domínio exemplo.com).
"""

from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

SEMENTE_CADASTROS = 42
QTD_PRODUTOS = 300
QTD_CLIENTES_BASE = 5000
ID_ECOMMERCE = "ECOM"

# --------------------------------------------------------------------------
# Listas de apoio
# --------------------------------------------------------------------------

DEPARTAMENTOS = {
    "Feminino": ["Blusas", "Calças", "Vestidos", "Saias", "Jaquetas"],
    "Masculino": ["Camisetas", "Camisas", "Calças", "Bermudas", "Jaquetas"],
    "Infantil": ["Conjuntos", "Camisetas", "Calças", "Vestidos"],
    "Bebê": ["Bodies", "Macacões", "Conjuntos"],
    "Moda Íntima": ["Sutiãs", "Calcinhas", "Cuecas", "Pijamas"],
    "Calçados": ["Tênis", "Sandálias", "Botas"],
    "Acessórios": ["Bolsas", "Bonés", "Cintos", "Bijuterias"],
}

FAIXA_PRECO = {
    "Feminino": (49.9, 229.9),
    "Masculino": (39.9, 199.9),
    "Infantil": (29.9, 129.9),
    "Bebê": (24.9, 99.9),
    "Moda Íntima": (19.9, 89.9),
    "Calçados": (79.9, 299.9),
    "Acessórios": (19.9, 179.9),
}

CORES = ["Preto", "Branco", "Azul", "Bege", "Verde", "Vermelho", "Rosa", "Cinza"]
TAMANHOS = {
    "Calçados": ["34", "36", "38", "40", "42"],
    "Acessórios": ["Único"],
    "Bebê": ["P", "M", "G"],
    "Infantil": ["4", "6", "8", "10", "12"],
}
TAMANHOS_PADRAO = ["PP", "P", "M", "G", "GG"]

CIDADES = [
    ("São Paulo", "SP", "Sudeste"), ("Campinas", "SP", "Sudeste"),
    ("Santos", "SP", "Sudeste"), ("Ribeirão Preto", "SP", "Sudeste"),
    ("Rio de Janeiro", "RJ", "Sudeste"), ("Niterói", "RJ", "Sudeste"),
    ("Belo Horizonte", "MG", "Sudeste"), ("Uberlândia", "MG", "Sudeste"),
    ("Curitiba", "PR", "Sul"), ("Londrina", "PR", "Sul"),
    ("Porto Alegre", "RS", "Sul"), ("Florianópolis", "SC", "Sul"),
    ("Joinville", "SC", "Sul"), ("Salvador", "BA", "Nordeste"),
    ("Recife", "PE", "Nordeste"), ("Fortaleza", "CE", "Nordeste"),
    ("Brasília", "DF", "Centro-Oeste"), ("Goiânia", "GO", "Centro-Oeste"),
    ("Manaus", "AM", "Norte"), ("Belém", "PA", "Norte"),
]

PRIMEIROS_NOMES = [
    "Ana", "Beatriz", "Camila", "Daniela", "Eduarda", "Fernanda", "Gabriela",
    "Helena", "Isabela", "Juliana", "Larissa", "Mariana", "Natália", "Patrícia",
    "Rafaela", "Sofia", "Thaís", "Vitória", "André", "Bruno", "Carlos", "Diego",
    "Eduardo", "Felipe", "Gustavo", "Henrique", "Igor", "João", "Lucas",
    "Marcelo", "Pedro", "Rafael", "Thiago", "Vinícius",
]
SOBRENOMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves",
    "Pereira", "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho",
    "Almeida", "Lopes", "Soares", "Fernandes", "Vieira", "Barbosa",
]

FORMAS_PAGAMENTO_PDV = [
    ("Cartão Trama", 0.25), ("Crédito", 0.35), ("Débito", 0.15),
    ("Pix", 0.20), ("Dinheiro", 0.05),
]
FORMAS_PAGAMENTO_ECOM = [
    ("Cartão Trama", 0.20), ("Crédito", 0.50), ("Pix", 0.25), ("Boleto", 0.05),
]

# --------------------------------------------------------------------------
# Cadastros (determinísticos)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Loja:
    id_loja: str
    nome: str
    tipo: str
    cidade: str
    uf: str
    regiao: str
    area_m2: int
    data_inauguracao: date


@dataclass(frozen=True)
class Produto:
    sku: str
    descricao: str
    departamento: str
    categoria: str
    cor: str
    tamanho: str
    preco_lista: float


@dataclass(frozen=True)
class Cliente:
    id_cliente: str
    nome: str
    cpf: str
    email: str
    data_nascimento: date
    cidade: str
    uf: str
    cartao_trama: bool
    aceita_marketing: bool
    data_cadastro: date


def _escolha_ponderada(rnd: random.Random, opcoes: list[tuple[str, float]]) -> str:
    valores, pesos = zip(*opcoes)
    return rnd.choices(valores, weights=pesos, k=1)[0]


def gerar_lojas() -> list[Loja]:
    """30 lojas físicas + 2 centros de distribuição (CDs) que atendem o e-commerce."""
    rnd = random.Random(SEMENTE_CADASTROS)
    sufixos = ["Centro", "Shopping Norte", "Shopping Sul", "Rua Principal", "Shopping Leste"]
    lojas = []
    for i in range(30):
        cidade, uf, regiao = CIDADES[i % len(CIDADES)]
        sufixo = sufixos[(i // len(CIDADES)) + rnd.randint(0, 2)]
        tipo = "Rua" if sufixo in ("Centro", "Rua Principal") else "Shopping"
        lojas.append(
            Loja(
                id_loja=f"L{i + 1:03d}",
                nome=f"Trama {cidade} {sufixo}",
                tipo=tipo,
                cidade=cidade,
                uf=uf,
                regiao=regiao,
                area_m2=rnd.randrange(600, 2400, 50),
                data_inauguracao=date(2005, 1, 1) + timedelta(days=rnd.randint(0, 6900)),
            )
        )
    lojas.append(Loja("CD01", "Centro de Distribuição Sudeste", "CD", "Cajamar", "SP", "Sudeste", 45000, date(2012, 3, 1)))
    lojas.append(Loja("CD02", "Centro de Distribuição Nordeste", "CD", "Jaboatão", "PE", "Nordeste", 28000, date(2018, 8, 1)))
    return lojas


def _singular(categoria: str) -> str:
    excecoes = {"Bodies": "Body", "Macacões": "Macacão", "Tênis": "Tênis"}
    if categoria in excecoes:
        return excecoes[categoria]
    return categoria[:-1] if categoria.endswith("s") else categoria


def gerar_produtos() -> list[Produto]:
    rnd = random.Random(SEMENTE_CADASTROS + 1)
    pares = [(dep, cat) for dep, cats in DEPARTAMENTOS.items() for cat in cats]
    produtos = []
    for i in range(QTD_PRODUTOS):
        departamento, categoria = pares[i % len(pares)]
        cor = rnd.choice(CORES)
        tamanho = rnd.choice(TAMANHOS.get(departamento, TAMANHOS_PADRAO))
        minimo, maximo = FAIXA_PRECO[departamento]
        # Preços de varejo terminam em ,90 ou ,99
        preco = round(rnd.uniform(minimo, maximo)) - rnd.choice([0.10, 0.01])
        produtos.append(
            Produto(
                sku=f"SKU{i + 1:05d}",
                descricao=f"{_singular(categoria)} {departamento} {cor}",
                departamento=departamento,
                categoria=categoria,
                cor=cor,
                tamanho=tamanho,
                preco_lista=round(preco, 2),
            )
        )
    return produtos


def _gerar_cliente(rnd: random.Random, id_cliente: str, data_cadastro: date) -> Cliente:
    nome = f"{rnd.choice(PRIMEIROS_NOMES)} {rnd.choice(SOBRENOMES)}"
    cidade, uf, _ = rnd.choice(CIDADES)
    usuario = nome.lower().replace(" ", ".")
    for antigo, novo in (("á", "a"), ("é", "e"), ("í", "i"), ("ó", "o"), ("ú", "u"), ("ã", "a"), ("ç", "c"), ("ê", "e")):
        usuario = usuario.replace(antigo, novo)
    return Cliente(
        id_cliente=id_cliente,
        nome=nome,
        cpf="".join(str(rnd.randint(0, 9)) for _ in range(11)),  # fictício
        email=f"{usuario}{rnd.randint(1, 999)}@exemplo.com",
        data_nascimento=date(1955, 1, 1) + timedelta(days=rnd.randint(0, 18000)),
        cidade=cidade,
        uf=uf,
        cartao_trama=rnd.random() < 0.3,
        aceita_marketing=rnd.random() < 0.6,
        data_cadastro=data_cadastro,
    )


def gerar_clientes_base() -> list[Cliente]:
    rnd = random.Random(SEMENTE_CADASTROS + 2)
    return [
        _gerar_cliente(rnd, f"C{i + 1:07d}", date(2015, 1, 1) + timedelta(days=rnd.randint(0, 3600)))
        for i in range(QTD_CLIENTES_BASE)
    ]


# Os cadastros são calculados uma vez por processo
LOJAS = gerar_lojas()
LOJAS_FISICAS = [loja for loja in LOJAS if loja.tipo != "CD"]
PRODUTOS = gerar_produtos()
CLIENTES_BASE = gerar_clientes_base()

# --------------------------------------------------------------------------
# Escrita de arquivos
# --------------------------------------------------------------------------


def _numero_br(valor: float) -> str:
    """Formato brasileiro: 1234.5 -> '1234,50' (como exportam muitos ERPs)."""
    return f"{valor:.2f}".replace(".", ",")


def _escrever_csv(caminho: Path, cabecalho: list[str], linhas: list[list]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.writer(arquivo, delimiter=";")
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)


def _escrever_jsonl(caminho: Path, registros: list[dict]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8") as arquivo:
        for registro in registros:
            arquivo.write(json.dumps(registro, ensure_ascii=False) + "\n")


def _cpf_sujo(rnd: random.Random, cpf: str) -> str:
    """Às vezes com máscara, às vezes sem, como chega de sistemas diferentes."""
    if rnd.random() < 0.5:
        return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:]}"
    return cpf


def _email_sujo(rnd: random.Random, email: str) -> str:
    sorteio = rnd.random()
    if sorteio < 0.10:
        return email.upper()
    if sorteio < 0.15:
        return f"  {email} "
    return email


def _cliente_para_crm(rnd: random.Random, cliente: Cliente, atualizado_em: datetime) -> dict:
    return {
        "id_cliente": cliente.id_cliente,
        "nome": cliente.nome if rnd.random() > 0.05 else cliente.nome.upper(),
        "cpf": _cpf_sujo(rnd, cliente.cpf),
        "email": _email_sujo(rnd, cliente.email),
        "data_nascimento": cliente.data_nascimento.isoformat(),
        "endereco": {"cidade": cliente.cidade, "uf": cliente.uf},
        "programa_fidelidade": {"cartao_trama": cliente.cartao_trama},
        "consentimentos": {"aceita_marketing": cliente.aceita_marketing},
        "data_cadastro": cliente.data_cadastro.isoformat(),
        "atualizado_em": atualizado_em.isoformat(),
    }


def _fator_dia(dia: date) -> float:
    """Mais movimento no fim de semana e no fim do mês (pagamento de salários)."""
    fator = 1.4 if dia.weekday() >= 5 else 1.0
    if dia.day >= 28 or dia.day <= 5:
        fator *= 1.15
    return fator


def _momento_local(rnd: random.Random, dia: date, hora_inicio: int, hora_fim: int) -> datetime:
    segundos = rnd.randint(hora_inicio * 3600, hora_fim * 3600 - 1)
    return datetime.combine(dia, time()) + timedelta(seconds=segundos)


def gerar_dia(dia: date, destino: str | Path, carga_inicial: bool = False) -> dict[str, int]:
    """Gera os arquivos de um dia de operação da Trama Store.

    Args:
        dia: data de referência.
        destino: pasta raiz da landing (ex.: /Volumes/trama_store/bronze/landing).
        carga_inicial: se True, também escreve o cadastro completo de lojas,
            produtos e clientes (primeiro dia da carga histórica).

    Returns:
        Quantidade de registros escrita por fonte.
    """
    destino = Path(destino)
    rnd = random.Random(int(dia.strftime("%Y%m%d")))
    sufixo = dia.strftime("%Y%m%d")
    contagens: dict[str, int] = {}

    # ---------------- ERP: lojas (só na carga inicial) ----------------
    if carga_inicial:
        linhas = [
            [loja.id_loja, loja.nome, loja.tipo, loja.cidade, loja.uf, loja.regiao, loja.area_m2,
             loja.data_inauguracao.isoformat()]
            for loja in LOJAS
        ]
        _escrever_csv(
            destino / "erp_lojas" / f"lojas_{sufixo}.csv",
            ["id_loja", "nome", "tipo", "cidade", "uf", "regiao", "area_m2", "data_inauguracao"],
            linhas,
        )
        contagens["erp_lojas"] = len(linhas)

    # ---------------- ERP: produtos (completo ou só alterados) ----------------
    momento_erp = datetime.combine(dia, time(5, 0))
    if carga_inicial:
        produtos_do_dia = [(p, p.preco_lista) for p in PRODUTOS]
    else:
        alterados = rnd.sample(PRODUTOS, k=6)  # reajustes e remarcações de preço
        produtos_do_dia = [(p, round(round(p.preco_lista * rnd.choice([0.8, 0.9, 1.1])) - 0.10, 2)) for p in alterados]
    linhas = [
        [p.sku, p.descricao, p.departamento, p.categoria, p.cor, p.tamanho, _numero_br(preco), momento_erp.isoformat(sep=" ")]
        for p, preco in produtos_do_dia
    ]
    _escrever_csv(
        destino / "erp_produtos" / f"produtos_{sufixo}.csv",
        ["sku", "descricao", "departamento", "categoria", "cor", "tamanho", "preco_lista", "atualizado_em"],
        linhas,
    )
    contagens["erp_produtos"] = len(linhas)

    # ---------------- CRM: clientes (completo ou novos + alterados) ----------------
    momento_crm = datetime.combine(dia, time(23, 30))
    novos = [
        _gerar_cliente(rnd, f"C{sufixo}{n:03d}", dia) for n in range(rnd.randint(30, 60))
    ]
    if carga_inicial:
        registros = [_cliente_para_crm(rnd, c, momento_crm) for c in CLIENTES_BASE + novos]
    else:
        alterados = rnd.sample(CLIENTES_BASE, k=20)
        registros = [_cliente_para_crm(rnd, c, momento_crm) for c in novos + alterados]
    _escrever_jsonl(destino / "crm_clientes" / f"clientes_{sufixo}.json", registros)
    contagens["crm_clientes"] = len(registros)

    clientes_do_dia = CLIENTES_BASE + novos
    # Uma parte dos clientes compra nos dois canais (omnichannel)
    clientes_omni = CLIENTES_BASE[: QTD_CLIENTES_BASE // 5]

    # ---------------- PDV: vendas das lojas físicas ----------------
    total_pdv = 0
    for loja in LOJAS_FISICAS:
        linhas = []
        qtd_cupons = int(rnd.randint(40, 110) * _fator_dia(dia))
        for n in range(qtd_cupons):
            id_cupom = f"{loja.id_loja}-{sufixo}-{n + 1:05d}"
            momento = _momento_local(rnd, dia, 10, 22)
            sorteio = rnd.random()
            if sorteio < 0.15:
                cliente = rnd.choice(clientes_omni)
            elif sorteio < 0.45:
                cliente = rnd.choice(clientes_do_dia)
            else:
                cliente = None  # cliente não se identificou no caixa
            pagamento = _escolha_ponderada(rnd, FORMAS_PAGAMENTO_PDV)
            vendedor = f"V{loja.id_loja[1:]}{rnd.randint(1, 12):02d}"
            for item in range(1, rnd.choices([1, 2, 3, 4, 5], weights=[35, 30, 18, 10, 7])[0] + 1):
                produto = rnd.choice(PRODUTOS)
                quantidade = rnd.choices([1, 2, 3], weights=[80, 15, 5])[0]
                desconto = round(produto.preco_lista * quantidade * rnd.choice([0, 0, 0, 0.1, 0.2]), 2)
                sku = produto.sku
                # Sujeira proposital
                if rnd.random() < 0.004:
                    sku = ""
                if rnd.random() < 0.003:
                    quantidade = rnd.choice([0, -1])
                linha = [
                    id_cupom, item, loja.id_loja, momento.strftime("%d/%m/%Y %H:%M:%S"),
                    cliente.id_cliente if cliente else "", sku, quantidade,
                    _numero_br(produto.preco_lista), _numero_br(desconto), pagamento, vendedor,
                ]
                linhas.append(linha)
                if rnd.random() < 0.01:  # reenvio duplicado do PDV
                    linhas.append(list(linha))
        _escrever_csv(
            destino / "pdv_vendas" / dia.isoformat() / f"pdv_{loja.id_loja}.csv",
            ["id_cupom", "item", "id_loja", "data_hora", "id_cliente", "sku", "quantidade",
             "preco_unitario", "desconto", "forma_pagamento", "id_vendedor"],
            linhas,
        )
        total_pdv += len(linhas)
    contagens["pdv_vendas"] = total_pdv

    # ---------------- E-commerce: eventos de pedidos ----------------
    eventos = []
    qtd_pedidos = int(rnd.randint(500, 750) * _fator_dia(dia))
    for n in range(qtd_pedidos):
        pedido_id = f"PED-{sufixo}-{n + 1:06d}"
        # O e-commerce registra horário em UTC (Brasília = UTC-3)
        criado_local = _momento_local(rnd, dia, 0, 24)
        criado_utc = (criado_local + timedelta(hours=3)).replace(tzinfo=timezone.utc)
        cliente = rnd.choice(clientes_omni) if rnd.random() < 0.35 else rnd.choice(clientes_do_dia)
        retira_loja = rnd.random() < 0.25
        loja_retirada = rnd.choice(LOJAS_FISICAS) if retira_loja else None
        itens = []
        for _ in range(rnd.choices([1, 2, 3, 4], weights=[45, 30, 15, 10])[0]):
            produto = rnd.choice(PRODUTOS)
            quantidade = rnd.choices([1, 2], weights=[85, 15])[0]
            itens.append({
                "sku": produto.sku,
                "quantidade": quantidade,
                "preco_unitario": produto.preco_lista,
                "desconto": round(produto.preco_lista * quantidade * rnd.choice([0, 0, 0.1, 0.15]), 2),
            })
        metodo = _escolha_ponderada(rnd, FORMAS_PAGAMENTO_ECOM)
        parcelas = rnd.choice([1, 1, 2, 3, 6, 10]) if metodo in ("Crédito", "Cartão Trama") else 1
        evento = {
            "pedido_id": pedido_id,
            "canal": rnd.choices(["app", "site"], weights=[60, 40])[0],
            "status": rnd.choices(["aprovado", "pagamento_pendente"], weights=[95, 5])[0],
            "criado_em": criado_utc.isoformat().replace("+00:00", "Z"),
            "atualizado_em": criado_utc.isoformat().replace("+00:00", "Z"),
            "cliente": {"id_cliente": cliente.id_cliente, "email": _email_sujo(rnd, cliente.email)},
            "itens": itens,
            "pagamento": {"metodo": metodo, "parcelas": parcelas},
            "entrega": {
                "modalidade": "retira_loja" if retira_loja else "entrega_domicilio",
                "id_loja_retirada": loja_retirada.id_loja if loja_retirada else None,
                "uf": loja_retirada.uf if loja_retirada else cliente.uf,
                "valor_frete": 0.0 if retira_loja else rnd.choice([0.0, 9.9, 14.9, 19.9]),
            },
        }
        eventos.append(evento)
        if rnd.random() < 0.005:  # mesmo evento enviado duas vezes
            eventos.append(json.loads(json.dumps(evento)))
        if rnd.random() < 0.03:  # cancelamento chega horas depois
            cancelado = json.loads(json.dumps(evento))
            cancelado["status"] = "cancelado"
            cancelado["atualizado_em"] = (criado_utc + timedelta(hours=rnd.randint(1, 20))).isoformat().replace("+00:00", "Z")
            eventos.append(cancelado)
    rnd.shuffle(eventos)  # eventos chegam fora de ordem
    _escrever_jsonl(destino / "ecommerce_pedidos" / f"pedidos_{sufixo}.json", eventos)
    contagens["ecommerce_pedidos"] = len(eventos)

    # ---------------- WMS: foto do estoque no fim do dia ----------------
    linhas = []
    for loja in LOJAS:
        for produto in PRODUTOS:
            if loja.tipo != "CD" and rnd.random() < 0.3:
                continue  # nem toda loja trabalha com todo o sortimento
            maximo = 400 if loja.tipo == "CD" else 40
            quantidade = 0 if rnd.random() < 0.04 else rnd.randint(1, maximo)
            linhas.append([dia.isoformat(), loja.id_loja, produto.sku, quantidade])
    _escrever_csv(destino / "estoque" / f"estoque_{sufixo}.csv", ["data_estoque", "id_local", "sku", "qtd_disponivel"], linhas)
    contagens["estoque"] = len(linhas)

    return contagens


def gerar_periodo(inicio: date, fim: date, destino: str | Path) -> dict[str, int]:
    """Gera vários dias. O primeiro dia inclui a carga inicial dos cadastros."""
    totais: dict[str, int] = {}
    dia = inicio
    while dia <= fim:
        for fonte, qtd in gerar_dia(dia, destino, carga_inicial=(dia == inicio)).items():
            totais[fonte] = totais.get(fonte, 0) + qtd
        dia += timedelta(days=1)
    return totais
