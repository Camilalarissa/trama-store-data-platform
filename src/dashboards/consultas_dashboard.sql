-- Consultas para montar o AI/BI Dashboard "Trama Store · Visão Omnichannel"
-- Use cada bloco como um dataset no dashboard (Dashboards → Create dashboard → Data).
-- Troque trama_store_dev pelo catálogo que você usou.

-- 1. Cartões de resumo (faturamento, vendas, ticket médio)
SELECT
  SUM(faturamento_liquido) AS faturamento_liquido,
  SUM(qtd_vendas) AS vendas,
  ROUND(SUM(faturamento_liquido) / SUM(qtd_vendas), 2) AS ticket_medio,
  ROUND(SUM(CASE WHEN tipo_canal = 'Digital' THEN faturamento_liquido END) / SUM(faturamento_liquido), 4) AS participacao_digital
FROM trama_store_dev.gold.kpi_vendas_diarias;

-- 2. Faturamento diário por canal (gráfico de linhas)
SELECT data_venda, canal, SUM(faturamento_liquido) AS faturamento_liquido
FROM trama_store_dev.gold.kpi_vendas_diarias
GROUP BY ALL
ORDER BY data_venda;

-- 3. Faturamento por região (barras); o e-commerce aparece como "Nacional"
SELECT regiao, tipo_canal, SUM(faturamento_liquido) AS faturamento_liquido
FROM trama_store_dev.gold.kpi_vendas_diarias
GROUP BY ALL
ORDER BY faturamento_liquido DESC;

-- 4. Ranking de lojas físicas (tabela)
SELECT loja, uf, SUM(qtd_vendas) AS vendas, SUM(faturamento_liquido) AS faturamento_liquido,
       ROUND(SUM(faturamento_liquido) / SUM(qtd_vendas), 2) AS ticket_medio
FROM trama_store_dev.gold.kpi_vendas_diarias
WHERE tipo_canal = 'Físico'
GROUP BY ALL
ORDER BY faturamento_liquido DESC;

-- 5. Mix por departamento e canal (barras empilhadas 100%)
SELECT tipo_canal, departamento, SUM(faturamento_liquido) AS faturamento_liquido
FROM trama_store_dev.gold.kpi_mix_departamento
GROUP BY ALL;

-- 6. Perfil de clientes (barras): o cliente omnichannel gasta mais?
SELECT perfil, clientes, compras_por_cliente, gasto_medio
FROM trama_store_dev.gold.kpi_clientes_perfil
ORDER BY gasto_medio DESC;

-- 7. Retirada na loja: quanto do e-commerce passa pelas lojas físicas
SELECT modalidade_entrega, COUNT(DISTINCT id_venda) AS pedidos, SUM(valor_liquido) AS faturamento_liquido
FROM trama_store_dev.gold.fato_vendas
WHERE tipo_canal = 'Digital'
GROUP BY ALL;

-- 8. Rupturas com demanda (tabela)
SELECT local, regiao, departamento, descricao, pecas_vendidas_7d
FROM trama_store_dev.gold.kpi_ruptura_estoque
ORDER BY pecas_vendidas_7d DESC;

-- 9. Qualidade dos dados (barras)
SELECT data_processamento, motivo_rejeicao, SUM(linhas_rejeitadas) AS linhas
FROM trama_store_dev.gold.kpi_qualidade_dados
GROUP BY ALL;
