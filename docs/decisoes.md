# Registro de decisões técnicas

Cada decisão segue o formato: **contexto → decisão → por quê → consequência**. Quando uma
decisão mudar, ela não é apagada: ganha uma nova entrada explicando a mudança.

---

## D01 · Marca fictícia e dados sintéticos

- **Contexto:** o projeto se inspira no modelo de negócio de grandes varejistas de moda
  brasileiros (lojas físicas + site + app + cartão próprio).
- **Decisão:** usar uma marca fictícia, a **Trama Store**, com dados gerados por código.
- **Por quê:** usar o nome de uma empresa real com dados inventados poderia parecer dado
  verdadeiro dela. Dados sintéticos também permitem controlar a "sujeira" para exercitar
  limpeza e qualidade.
- **Consequência:** nomes, CPFs e e-mails são inventados (domínio `exemplo.com`).

## D02 · Databricks Free Edition com compute serverless

- **Decisão:** rodar tudo no Free Edition, sem configuração de cluster.
- **Por quê:** é gratuito e cobre Unity Catalog, Volumes, Auto Loader, Jobs e dashboards.
- **Consequência:** sem `cache()`/`persist()` (não suportados no serverless) e no máximo
  5 tasks simultâneas por conta. O Job foi desenhado com no máximo 3 tasks em paralelo.

## D03 · Arquitetura Medallion com Unity Catalog

- **Decisão:** um catálogo por ambiente (`trama_store_dev`, `trama_store`) com os schemas
  `bronze`, `silver` e `gold`. Arquivos recebidos ficam no Volume `bronze.landing`.
- **Por quê:** separa responsabilidades por camada e permite dar permissões por schema
  (ex.: analistas só leem a `gold`).

## D04 · Bronze guarda tudo como texto

- **Decisão:** a Bronze não converte tipos (o Auto Loader lê CSV e JSON como texto).
- **Por quê:** a Bronze é a cópia fiel do que chegou. Se uma conversão estiver errada,
  corrige-se a Silver e reprocessa, sem precisar buscar os arquivos de novo.
- **Consequência:** toda a tipagem acontece nas funções de `src/trama/transformacoes.py`.

## D05 · Carga incremental com `foreachBatch` + `MERGE`

- **Decisão:** Bronze → Silver com Structured Streaming (`trigger(availableNow=True)`) e
  `MERGE` pela chave de negócio dentro do `foreachBatch`.
- **Por quê:** o checkpoint faz cada execução ler só o que é novo, e o MERGE torna a carga
  **idempotente**: reprocessar não duplica.
- **Consequência:** atualizações tardias são tratadas com `coluna_versao`: o cancelamento de
  um pedido só sobrescreve a linha se for mais recente (`atualizado_em` maior).

## D06 · Uma tabela de vendas para os dois canais (omnichannel)

- **Decisão:** PDV e e-commerce viram uma única `silver.vendas_itens`, com o mesmo esquema
  e a coluna `canal` (`loja_fisica`, `site`, `app`).
- **Por quê:** a pergunta central do negócio é comparar e somar canais. Um esquema único
  evita `UNION` repetido em cada análise.
- **Atribuição:** venda online com retirada na loja conta como venda **digital**
  (`id_loja_venda = 'ECOM'`); a loja de retirada fica em `id_loja_retirada`. Assim dá para
  medir quanto do digital passa pelas lojas sem contar a venda duas vezes.

## D07 · Datas e horas em `TIMESTAMP_NTZ` no horário de Brasília

- **Contexto:** o PDV manda horário local; o e-commerce manda UTC.
- **Decisão:** converter tudo para o horário de Brasília e guardar como `TIMESTAMP_NTZ`
  (sem fuso).
- **Por quê:** um `TIMESTAMP` comum é um instante absoluto e é exibido no fuso de quem lê.
  Os testes mostraram isso: a mesma venda aparecia 3 horas antes numa máquina configurada
  em outro fuso. Com NTZ, "20h17" é sempre 20h17, e `data_venda` sempre cai no dia certo.

## D08 · Qualidade de dados com quarentena

- **Decisão:** regras de qualidade (`REGRAS_VENDAS`) separam as linhas reprovadas para
  `silver.vendas_rejeitadas`, com o motivo, em vez de descartá-las.
- **Por quê:** descartar em silêncio esconde problemas da fonte. A quarentena permite
  medir (`gold.kpi_qualidade_dados`), investigar e reprocessar.

## D09 · LGPD: CPF como hash e Gold sem dados pessoais

- **Decisão:** a Silver guarda só `cpf_hash` (SHA-256); a `gold.dim_cliente` não tem nome,
  e-mail nem CPF.
- **Limitação conhecida:** um hash de CPF sem "sal" pode ser revertido por força bruta
  (são só 10¹¹ combinações). Em produção, usar um sal guardado em `dbutils.secrets` ou um
  serviço de tokenização. Os dados aqui são fictícios.

## D10 · Funções puras e testadas fora do Databricks

- **Decisão:** toda a lógica de transformação fica em `src/trama/` como funções
  `DataFrame → DataFrame`, e os notebooks só orquestram.
- **Por quê:** permite testar com `pytest` e Spark local, no computador e no GitHub
  Actions, inclusive executando o SQL da Gold.
- **Consequência:** no serverless, o código do `foreachBatch` roda no servidor; por isso o
  pacote é registrado com `cloudpickle.register_pickle_by_value` em `_comum.py`.

## D11 · Gold recriada a cada execução

- **Decisão:** as tabelas Gold usam `CREATE OR REPLACE TABLE`.
- **Por quê:** com o volume atual, recriar é rápido, simples e sempre coerente com a
  Silver. Quando o volume crescer, migrar para **materialized views** com atualização
  incremental (ver roadmap).
