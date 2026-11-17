from trama.incremental import montar_merge_sql


def test_merge_com_versao_so_atualiza_se_mais_recente():
    sql = montar_merge_sql("silver.vendas_itens", "lote", ["id_venda", "id_item"], "atualizado_em")
    assert "ON t.id_venda <=> s.id_venda AND t.id_item <=> s.id_item" in sql
    assert "WHEN MATCHED AND s.atualizado_em > t.atualizado_em THEN UPDATE SET *" in sql
    assert sql.endswith("WHEN NOT MATCHED THEN INSERT *")


def test_merge_somente_inserir_nunca_atualiza():
    sql = montar_merge_sql("silver.vendas_rejeitadas", "lote", ["id_venda", "id_item"], somente_inserir=True)
    assert "WHEN MATCHED" not in sql
    assert "INSERT *" in sql
