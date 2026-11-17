"""Catálogo das fontes de dados: pasta na landing, formato e opções de leitura.

Usado pelo notebook da Bronze (Auto Loader) e pelos testes (leitura em lote),
para que os dois leiam os arquivos exatamente do mesmo jeito.

Na Bronze, todas as colunas chegam como texto (inclusive no JSON, que é o padrão
do Auto Loader sem `cloudFiles.inferColumnTypes`). A tipagem acontece na Silver.
Assim a Bronze guarda o dado como ele veio, sem perdas de conversão.
"""

FONTES = {
    "erp_lojas": {"formato": "csv", "opcoes": {"header": "true", "sep": ";"}},
    "erp_produtos": {"formato": "csv", "opcoes": {"header": "true", "sep": ";"}},
    "crm_clientes": {"formato": "json", "opcoes": {}},
    "pdv_vendas": {"formato": "csv", "opcoes": {"header": "true", "sep": ";"}},
    "ecommerce_pedidos": {"formato": "json", "opcoes": {}},
    "estoque": {"formato": "csv", "opcoes": {"header": "true", "sep": ";"}},
}
