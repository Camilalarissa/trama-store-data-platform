# Roadmap

O projeto evolui em fases. Cada fase acrescenta uma área do negócio e um conjunto de
técnicas, alinhado aos domínios da certificação **Databricks Data Engineer Associate**.

## ✅ Fase 1 · MVP (atual)

- [x] Gerador de dados sintéticos com sujeira proposital
- [x] Bronze com Auto Loader e evolução de schema
- [x] Silver incremental e idempotente (`foreachBatch` + `MERGE`)
- [x] Vendas omnichannel unificadas (PDV + site + app)
- [x] Quarentena de dados inválidos
- [x] Gold em modelo estrela + 5 tabelas de KPI
- [x] Job orquestrado com Bundle (dev e prod)
- [x] Testes com pytest + GitHub Actions

## Fase 2 · Visualização e autoatendimento

- [ ] AI/BI Dashboard "Visão Omnichannel" (consultas em `src/dashboards/`)
- [ ] Genie Space para perguntas em linguagem natural sobre a Gold
- [ ] Comentários em todas as tabelas e colunas (documentação no Unity Catalog)

## Fase 3 · Novas áreas do negócio

- [ ] **Devoluções e trocas** (loja e online), com impacto no faturamento líquido
- [ ] **Logística**: expedição dos CDs, prazo prometido × entregue, custo de frete
- [ ] **Marketing**: campanhas, cupons e atribuição de vendas
- [ ] **Cartão Trama (financeiro)**: faturas, parcelamento e inadimplência
- [ ] **Pessoas (RH)**: vendedores, metas e comissões por loja

## Fase 4 · Técnicas avançadas

- [ ] Migrar a Silver para **Lakeflow Spark Declarative Pipelines** com *expectations*
- [ ] **SCD tipo 2** em produtos para guardar o histórico de preços
- [ ] Gold com **materialized views** (atualização incremental)
- [ ] Permissões por grupo no Unity Catalog, **filtro de linha** por região e **máscara** de e-mail
- [ ] Monitoramento: alertas de SQL para queda de faturamento e aumento de rejeições
- [ ] Deploy automático em `prod` após merge na `main`

## Fase 5 · IA

- [ ] Avaliações de clientes com análise de sentimento usando `ai_query`
- [ ] Previsão de demanda por SKU e loja para reduzir rupturas
