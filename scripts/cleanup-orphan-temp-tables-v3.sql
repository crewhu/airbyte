-- =============================================================================
-- Limpeza de tabelas temporárias órfãs — destination-postgres v3 (direct load)
--
-- Alvo: temp tables deixadas por jobs que falharam antes da 3.0.5-crewhu.2,
-- que corrigiu o vazamento na origem. Como o nome carrega um id único por
-- connection, nenhum job posterior as reaproveita — elas só acumulam.
--
-- Nome gerado por DefaultTempTableNameGenerator:
--     <namespace[0:8]><nome[0:8]><sha256hex[0:32]>
-- ex.: airbyte_etltrends_calendars2e8b1f09e5b410f9bbb28eba9943bf75
--
-- GARANTIAS DE QUE NENHUMA TABELA REAL É APAGADA
-- ----------------------------------------------
-- Uma tabela só é candidata se satisfizer TODAS as condições abaixo.
-- Nenhuma tabela final satisfaz as três ao mesmo tempo:
--
--   1. Termina em exatamente 32 caracteres hex.
--      Tabela final tem nome legível (trends_calendars, auto_task_Tickets).
--
--   2. Começa com o prefixo do schema (airbyte_etl...).
--      É o <namespace[0:8]> do gerador. Tabela final não tem esse prefixo.
--
--   3. NÃO tem lock ativo.
--      Se um sync estiver usando, fica de fora.
--
--   4. Não é _airbyte_connection_test_* nem _ab_soft_reset (o Airbyte gerencia).
--
-- Além disso o PASSO 1 é somente leitura e o PASSO 3 confirma, depois da
-- limpeza, que a contagem de tabelas finais não mudou.
--
-- Uso: rode o PASSO 1, confira a lista, depois rode o PASSO 2.
-- =============================================================================


-- =============================================================================
-- PASSO 1 — CONFERÊNCIA (somente leitura). Rode primeiro e leia a lista.
-- =============================================================================

SELECT c.relname AS tabela,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS tamanho
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname ~ '[0-9a-f]{32}$'              -- (1) hash de 32 hex no fim
  AND c.relname LIKE 'airbyte_etl%'            -- (2) prefixo do namespace
  AND c.relname NOT LIKE '\_airbyte\_connection\_test\_%'   -- (4)
  AND c.relname !~ '_ab_soft_reset$'                        -- (4)
  AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid)  -- (3)
ORDER BY pg_total_relation_size(c.oid) DESC;

-- Total a ser liberado:
SELECT count(*) AS qtd_tabelas,
       pg_size_pretty(COALESCE(sum(pg_total_relation_size(c.oid)), 0)) AS tamanho_total
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname ~ '[0-9a-f]{32}$'
  AND c.relname LIKE 'airbyte_etl%'
  AND c.relname NOT LIKE '\_airbyte\_connection\_test\_%'
  AND c.relname !~ '_ab_soft_reset$'
  AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid);

-- Quantas tabelas finais existem hoje (anote para comparar no PASSO 3):
SELECT count(*) AS tabelas_finais
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname !~ '[0-9a-f]{32}$';


-- =============================================================================
-- PASSO 2 — EXECUÇÃO. Rode o bloco inteiro (auto-commit ligado, padrão pgAdmin).
--
-- Um DROP por vez: cada um pega ACCESS EXCLUSIVE, e segurar todos numa
-- transação travaria o schema até o fim. lock_timeout de 5s faz o DROP
-- desistir em vez de enfileirar atrás de um sync — o que for pulado aparece
-- no fim e pode ser retomado rodando de novo.
-- =============================================================================

DO $$
DECLARE
  r        record;
  dropped  int := 0;
  skipped  int := 0;
BEGIN
  SET lock_timeout = '5s';

  FOR r IN
    SELECT c.relname, pg_total_relation_size(c.oid) AS bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname = 'airbyte_etl'
      AND c.relname ~ '[0-9a-f]{32}$'
      AND c.relname LIKE 'airbyte_etl%'
      AND c.relname NOT LIKE '\_airbyte\_connection\_test\_%'
      AND c.relname !~ '_ab_soft_reset$'
      AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid)
    ORDER BY pg_total_relation_size(c.oid) DESC
  LOOP
    BEGIN
      EXECUTE format('DROP TABLE IF EXISTS %I.%I', 'airbyte_etl', r.relname);
      dropped := dropped + 1;
      IF dropped % 100 = 0 THEN
        RAISE NOTICE 'Progresso: % dropadas, % puladas...', dropped, skipped;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      skipped := skipped + 1;
      RAISE NOTICE 'PULADA % (%) — %', r.relname, pg_size_pretty(r.bytes), SQLERRM;
    END;
    COMMIT;
  END LOOP;

  RAISE NOTICE 'Concluído: % dropadas, % puladas.', dropped, skipped;
END
$$;


-- =============================================================================
-- PASSO 3 — VERIFICAÇÃO. A contagem de tabelas finais tem que ser IDÊNTICA
-- à do PASSO 1. Se diminuiu, algo real foi apagado — restaure do backup.
-- =============================================================================

SELECT count(*) AS tabelas_finais
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname !~ '[0-9a-f]{32}$';

-- Sobras (só deve restar o que estava em uso durante a limpeza):
SELECT c.relname AS tabela,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS tamanho,
       EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid) AS em_uso
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname ~ '[0-9a-f]{32}$'
  AND c.relname LIKE 'airbyte_etl%'
ORDER BY pg_total_relation_size(c.oid) DESC;
