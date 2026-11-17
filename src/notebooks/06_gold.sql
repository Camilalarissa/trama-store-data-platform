-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Gold: modelo dimensional e indicadores
-- MAGIC
-- MAGIC Tabelas prontas para dashboards, Genie e análises. Todas são recriadas a cada execução
-- MAGIC (`CREATE OR REPLACE`), o que as mantém idempotentes e sempre coerentes com a Silver.
-- MAGIC
-- MAGIC | Tabela | Grão (uma linha por...) | Pergunta de negócio |
-- MAGIC |---|---|---|
-- MAGIC | `dim_data`, `dim_loja`, `dim_produto`, `dim_cliente` | data / loja / SKU / cliente | Dimensões do modelo estrela |
-- MAGIC | `fato_vendas` | item vendido (só vendas válidas) | Base de todas as métricas |
-- MAGIC | `kpi_vendas_diarias` | dia × canal × loja | Quanto vendemos, onde e com que ticket? |
-- MAGIC | `kpi_mix_departamento` | dia × tipo de canal × departamento | O que vende mais em cada canal? |
-- MAGIC | `kpi_clientes_perfil` | perfil de cliente | Cliente omnichannel gasta mais? |
-- MAGIC | `kpi_ruptura_estoque` | local × SKU zerado | Onde falta produto que tem demanda? |
-- MAGIC | `kpi_qualidade_dados` | dia × canal × motivo | Quanto dado chega com problema? |
-- MAGIC
-- MAGIC **Privacidade:** a `dim_cliente` não tem nome, e-mail nem CPF; só atributos analíticos.

-- COMMAND ----------

CREATE WIDGET TEXT catalogo DEFAULT 'trama_store_dev';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalogo);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensões

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_data
COMMENT 'Calendário de 2025 a 2027'
AS
SELECT
  d AS data,
  year(d) AS ano,
  quarter(d) AS trimestre,
  month(d) AS mes,
  date_format(d, 'yyyy-MM') AS ano_mes,
  day(d) AS dia,
  dayofweek(d) AS dia_semana_num,
  CASE dayofweek(d)
    WHEN 1 THEN 'Domingo' WHEN 2 THEN 'Segunda' WHEN 3 THEN 'Terça' WHEN 4 THEN 'Quarta'
    WHEN 5 THEN 'Quinta' WHEN 6 THEN 'Sexta' ELSE 'Sábado'
  END AS dia_semana,
  dayofweek(d) IN (1, 7) AS fim_de_semana
FROM (SELECT explode(sequence(DATE'2025-01-01', DATE'2027-12-31', INTERVAL 1 DAY)) AS d);

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_loja
COMMENT 'Lojas físicas, centros de distribuição e o e-commerce como loja virtual (ECOM)'
AS
SELECT id_loja, nome, tipo, cidade, uf, regiao, area_m2, data_inauguracao
FROM silver.lojas
UNION ALL
SELECT 'ECOM', 'E-commerce (site e app)', 'Digital', NULL, NULL, 'Nacional', NULL, NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_produto AS
SELECT sku, descricao, departamento, categoria, cor, tamanho, preco_lista
FROM silver.produtos;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_cliente
COMMENT 'Atributos analíticos do cliente, sem dados pessoais identificáveis'
AS
SELECT
  id_cliente,
  CASE
    WHEN idade < 25 THEN '18-24'
    WHEN idade < 35 THEN '25-34'
    WHEN idade < 45 THEN '35-44'
    WHEN idade < 60 THEN '45-59'
    ELSE '60+'
  END AS faixa_etaria,
  cidade,
  uf,
  cartao_trama,
  aceita_marketing,
  data_cadastro
FROM (
  SELECT *, floor(months_between(current_date(), data_nascimento) / 12) AS idade
  FROM silver.clientes
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fato

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fato_vendas
CLUSTER BY (data_venda)
COMMENT 'Itens vendidos válidos (exclui pedidos cancelados e com pagamento pendente)'
AS
SELECT
  data_venda,
  data_hora_venda,
  id_venda,
  id_item,
  canal,
  CASE WHEN canal = 'loja_fisica' THEN 'Físico' ELSE 'Digital' END AS tipo_canal,
  id_loja_venda,
  id_loja_retirada,
  modalidade_entrega,
  id_cliente,
  sku,
  quantidade,
  preco_unitario,
  valor_bruto,
  desconto,
  valor_liquido,
  forma_pagamento
FROM silver.vendas_itens
WHERE status IN ('concluida', 'aprovado');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Indicadores (KPIs)

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.kpi_vendas_diarias
COMMENT 'Atenção: ticket_medio não pode ser somado nem tirado média entre linhas; recalcule a partir de faturamento e vendas.'
AS
SELECT
  f.data_venda,
  f.canal,
  f.tipo_canal,
  f.id_loja_venda AS id_loja,
  l.nome AS loja,
  l.regiao,
  l.uf,
  COUNT(DISTINCT f.id_venda) AS qtd_vendas,
  SUM(f.quantidade) AS qtd_pecas,
  SUM(f.valor_bruto) AS faturamento_bruto,
  SUM(f.desconto) AS total_descontos,
  SUM(f.valor_liquido) AS faturamento_liquido,
  ROUND(SUM(f.valor_liquido) / COUNT(DISTINCT f.id_venda), 2) AS ticket_medio
FROM gold.fato_vendas f
LEFT JOIN gold.dim_loja l ON f.id_loja_venda = l.id_loja
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.kpi_mix_departamento AS
SELECT
  data_venda,
  tipo_canal,
  departamento,
  qtd_pecas,
  faturamento_liquido,
  ROUND(faturamento_liquido / SUM(faturamento_liquido) OVER (PARTITION BY data_venda, tipo_canal), 4) AS participacao
FROM (
  SELECT f.data_venda, f.tipo_canal, p.departamento,
         SUM(f.quantidade) AS qtd_pecas, SUM(f.valor_liquido) AS faturamento_liquido
  FROM gold.fato_vendas f
  JOIN gold.dim_produto p ON f.sku = p.sku
  GROUP BY ALL
);

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.kpi_clientes_perfil
COMMENT 'Compara clientes que compram só na loja, só no digital ou nos dois canais'
AS
WITH por_cliente AS (
  SELECT
    id_cliente,
    MAX(CASE WHEN tipo_canal = 'Físico' THEN 1 ELSE 0 END) AS comprou_fisico,
    MAX(CASE WHEN tipo_canal = 'Digital' THEN 1 ELSE 0 END) AS comprou_digital,
    COUNT(DISTINCT id_venda) AS compras,
    SUM(valor_liquido) AS gasto
  FROM gold.fato_vendas
  WHERE id_cliente IS NOT NULL
  GROUP BY id_cliente
)
SELECT
  CASE
    WHEN comprou_fisico = 1 AND comprou_digital = 1 THEN 'Omnichannel'
    WHEN comprou_fisico = 1 THEN 'Só loja física'
    ELSE 'Só digital'
  END AS perfil,
  COUNT(*) AS clientes,
  ROUND(AVG(compras), 2) AS compras_por_cliente,
  ROUND(AVG(gasto), 2) AS gasto_medio,
  SUM(gasto) AS gasto_total
FROM por_cliente
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.kpi_ruptura_estoque
COMMENT 'Produtos zerados no último dia de estoque que venderam nos 7 dias anteriores. CDs usam a demanda do e-commerce.'
AS
WITH ultimo_dia AS (
  SELECT MAX(data_estoque) AS data_estoque FROM silver.estoque
),
estoque_atual AS (
  SELECT e.*
  FROM silver.estoque e
  JOIN ultimo_dia u ON e.data_estoque = u.data_estoque
),
demanda_7d AS (
  SELECT
    CASE WHEN f.tipo_canal = 'Físico' THEN f.id_loja_venda ELSE 'CD' END AS local_demanda,
    f.sku,
    SUM(f.quantidade) AS pecas_vendidas_7d
  FROM gold.fato_vendas f
  CROSS JOIN ultimo_dia u
  WHERE f.data_venda > date_sub(u.data_estoque, 7) AND f.data_venda <= u.data_estoque
  GROUP BY ALL
)
SELECT
  e.data_estoque,
  e.id_local,
  l.nome AS local,
  l.tipo AS tipo_local,
  l.regiao,
  e.sku,
  p.descricao,
  p.departamento,
  d.pecas_vendidas_7d
FROM estoque_atual e
JOIN gold.dim_loja l ON e.id_local = l.id_loja
JOIN gold.dim_produto p ON e.sku = p.sku
JOIN demanda_7d d
  ON d.sku = e.sku
 AND d.local_demanda = CASE WHEN l.tipo = 'CD' THEN 'CD' ELSE e.id_local END
WHERE e.qtd_disponivel = 0;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.kpi_qualidade_dados AS
SELECT
  to_date(processado_em) AS data_processamento,
  canal,
  motivo_rejeicao,
  COUNT(*) AS linhas_rejeitadas
FROM silver.vendas_rejeitadas
GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência rápida

-- COMMAND ----------

SELECT tipo_canal, canal, SUM(faturamento_liquido) AS faturamento, SUM(qtd_vendas) AS vendas
FROM gold.kpi_vendas_diarias
GROUP BY ALL
ORDER BY faturamento DESC;

-- COMMAND ----------

SELECT * FROM gold.kpi_clientes_perfil ORDER BY gasto_medio DESC;
