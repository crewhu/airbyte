-- =============================================================================
-- Limpeza de TODAS as tabelas temporárias órfãs — banco airbyte_etl_v3 (pgAdmin)
-- Contexto: INVESTIGACAO-STORAGE-AIRBYTEETL.md (~41.845 tabelas, ~62 GB)
--
-- Regras de segurança (NUNCA tocam em tabela final):
--   * Só tabelas do schema airbyte_etl cujo nome termina em _[0-9a-f]{8}
--     (hash de 8 hex gerado pelo mecanismo de temp table, commit 2fcf447e012)
--     E cujo nome-base exista como tabela real no mesmo schema
--     (trends_tickets_9455993b só entra porque trends_tickets existe).
--     Uma tabela final nunca casa as duas condições ao mesmo tempo.
--   * Exclui sufixos compostos SÓ de dígitos (ex.: _20250101): protege tabelas
--     reais particionadas/versionadas por data. Custo: deixa para trás ~2% das
--     temporárias (hash aleatório todo-numérico), que podem ser removidas
--     depois numa conferência manual.
--   * Ignora tabelas com lock ativo (sync em andamento usando a temp table).
--   * lock_timeout de 5s: se um sync pegar a tabela no meio, o DROP falha e a
--     tabela é pulada, sem travar nada. O sync afetado apenas refaz o batch.
--
-- Recomendação: rode fora do horário de sync pesado, ou pause as connections.
-- =============================================================================

-- PASSO 1 — CONFERÊNCIA (somente leitura): liste o que será apagado
SELECT n.nspname AS schema,
       c.relname AS tabela,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS tamanho
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname ~ '_[0-9a-f]{8}$'
  AND c.relname !~ '_[0-9]{8}$'          -- protege sufixos tipo data (_20250101)
  AND EXISTS (
        SELECT 1
        FROM pg_class base
        JOIN pg_namespace basen ON basen.oid = base.relnamespace
        WHERE basen.nspname = n.nspname
          AND base.relkind = 'r'
          AND base.relname = regexp_replace(c.relname, '_[0-9a-f]{8}$', '')
      )
  AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid)
ORDER BY pg_total_relation_size(c.oid) DESC;

-- Confira também o total (deve bater com ~41.8k tabelas / ~62 GB):
SELECT count(*) AS qtd_tabelas,
       pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS tamanho_total
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname ~ '_[0-9a-f]{8}$'
  AND c.relname !~ '_[0-9]{8}$'
  AND EXISTS (
        SELECT 1
        FROM pg_class base
        JOIN pg_namespace basen ON basen.oid = base.relnamespace
        WHERE basen.nspname = n.nspname
          AND base.relkind = 'r'
          AND base.relname = regexp_replace(c.relname, '_[0-9a-f]{8}$', '')
      );

-- =============================================================================
-- PASSO 2 — EXECUÇÃO: rode o bloco abaixo inteiro no Query Tool do pgAdmin
-- (deixe o auto-commit ligado, que é o padrão do pgAdmin).
-- Cada DROP é commitado individualmente; tabelas em uso são puladas.
-- Progresso a cada 500 tabelas na aba "Messages" (RAISE NOTICE).
-- São ~41k tabelas: espere alguns minutos. Pode interromper e rodar de novo
-- a qualquer momento — o que já foi dropado ficou dropado.
-- =============================================================================

DO $$
DECLARE
  r        record;
  dropped  int := 0;
  skipped  int := 0;
BEGIN
  SET lock_timeout = '5s';

  FOR r IN
    SELECT n.nspname, c.relname,
           pg_total_relation_size(c.oid) AS bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname = 'airbyte_etl'
      AND c.relname ~ '_[0-9a-f]{8}$'
      AND c.relname !~ '_[0-9]{8}$'      -- protege sufixos tipo data (_20250101)
      AND EXISTS (
            SELECT 1
            FROM pg_class base
            JOIN pg_namespace basen ON basen.oid = base.relnamespace
            WHERE basen.nspname = n.nspname
              AND base.relkind = 'r'
              AND base.relname = regexp_replace(c.relname, '_[0-9a-f]{8}$', '')
          )
      AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid)
    ORDER BY pg_total_relation_size(c.oid) DESC
  LOOP
    BEGIN
      EXECUTE format('DROP TABLE IF EXISTS %I.%I', r.nspname, r.relname);
      dropped := dropped + 1;
      IF r.bytes >= 50 * 1024 * 1024 THEN
        RAISE NOTICE 'DROP ok   %.% (%)', r.nspname, r.relname, pg_size_pretty(r.bytes);
      ELSIF dropped % 500 = 0 THEN
        RAISE NOTICE 'Progresso: % dropadas, % puladas...', dropped, skipped;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      skipped := skipped + 1;
      RAISE NOTICE 'PULADA    %.% (%) — em uso: %', r.nspname, r.relname,
                   pg_size_pretty(r.bytes), SQLERRM;
    END;
    COMMIT;  -- libera o espaço e os locks a cada tabela
  END LOOP;

  RAISE NOTICE 'Concluído: % dropadas, % puladas.', dropped, skipped;
END
$$;

-- =============================================================================
-- PASSO 3 — VERIFICAÇÃO PÓS-LIMPEZA
-- =============================================================================

-- 3a. Sobras (deve restar só o que estava em uso ou com sufixo todo-numérico):
SELECT n.nspname AS schema,
       c.relname AS tabela,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS tamanho,
       EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid) AS em_uso
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname ~ '_[0-9a-f]{8}$'
  AND EXISTS (
        SELECT 1
        FROM pg_class base
        JOIN pg_namespace basen ON basen.oid = base.relnamespace
        WHERE basen.nspname = n.nspname
          AND base.relkind = 'r'
          AND base.relname = regexp_replace(c.relname, '_[0-9a-f]{8}$', '')
      )
ORDER BY pg_total_relation_size(c.oid) DESC;

-- 3b. Confirme que as tabelas finais continuam intactas:
SELECT count(*) AS tabelas_finais
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname !~ '_[0-9a-f]{8}$';

-- =============================================================================
-- PASSO 4 — SOBRAS COM SUFIXO TODO-NUMÉRICO (os ~2% deixados pelo PASSO 2)
--
-- Conferido em 2026-09-10: restaram 971 tabelas (~514 MB) com sufixo de 8
-- dígitos + 42 (~84 MB) começando com 19/20. NENHUM sufixo é uma data válida
-- YYYYMMDD (ex.: 19765421 = mês 76, 20528469 = mês 52) — são hashes aleatórios
-- que saíram todo-numéricos.
--
-- Mesmas regras do PASSO 2, com uma proteção mais precisa no lugar do
-- "!~ _[0-9]{8}$": exclui só sufixos que sejam DATA VÁLIDA de calendário
-- (_YYYYMMDD com ano 19xx/20xx, mês 01-12, dia 01-31). Tabela particionada
-- por data continua intocável; hash numérico aleatório cai na limpeza.
-- =============================================================================

-- 4a. CONFERÊNCIA (somente leitura):
  SELECT n.nspname AS schema,
        c.relname AS tabela,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS tamanho
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind = 'r'
    AND n.nspname = 'airbyte_etl'
    AND c.relname ~ '_[0-9]{8}$'
    AND c.relname !~ '_(19|20)[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$'  -- protege datas reais
    AND EXISTS (
          SELECT 1
          FROM pg_class base
          JOIN pg_namespace basen ON basen.oid = base.relnamespace
          WHERE basen.nspname = n.nspname
            AND base.relkind = 'r'
            AND base.relname = regexp_replace(c.relname, '_[0-9]{8}$', '')
        )
    AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid)
  ORDER BY pg_total_relation_size(c.oid) DESC;

-- 4b. EXECUÇÃO (mesmo mecanismo do PASSO 2: auto-commit, pula tabela em uso):
DO $$
DECLARE
  r        record;
  dropped  int := 0;
  skipped  int := 0;
BEGIN
  SET lock_timeout = '5s';

  FOR r IN
    SELECT n.nspname, c.relname,
           pg_total_relation_size(c.oid) AS bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname = 'airbyte_etl'
      AND c.relname ~ '_[0-9]{8}$'
      AND c.relname !~ '_(19|20)[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$'
      AND EXISTS (
            SELECT 1
            FROM pg_class base
            JOIN pg_namespace basen ON basen.oid = base.relnamespace
            WHERE basen.nspname = n.nspname
              AND base.relkind = 'r'
              AND base.relname = regexp_replace(c.relname, '_[0-9]{8}$', '')
          )
      AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid)
    ORDER BY pg_total_relation_size(c.oid) DESC
  LOOP
    BEGIN
      EXECUTE format('DROP TABLE IF EXISTS %I.%I', r.nspname, r.relname);
      dropped := dropped + 1;
      IF r.bytes >= 50 * 1024 * 1024 THEN
        RAISE NOTICE 'DROP ok   %.% (%)', r.nspname, r.relname, pg_size_pretty(r.bytes);
      ELSIF dropped % 100 = 0 THEN
        RAISE NOTICE 'Progresso: % dropadas, % puladas...', dropped, skipped;
      END IF;
    EXCEPTION WHEN OTHERS THEN
      skipped := skipped + 1;
      RAISE NOTICE 'PULADA    %.% (%) — em uso: %', r.nspname, r.relname,
                   pg_size_pretty(r.bytes), SQLERRM;
    END;
    COMMIT;
  END LOOP;

  RAISE NOTICE 'Concluído: % dropadas, % puladas.', dropped, skipped;
END
$$;

-- =============================================================================
-- PASSO 5 (opcional) — TABELAS _ab_soft_reset ÓRFÃS
-- Conferido em 2026-09-10: 4 tabelas (~58 MB). São temporárias do soft reset
-- do Airbyte; sem lock ativo, são sobras de resets antigos e o Airbyte recria
-- se precisar. Rode a conferência e, se estiver ok, o bloco de DROP.
-- =============================================================================

-- 5a. CONFERÊNCIA:
SELECT n.nspname AS schema,
       c.relname AS tabela,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS tamanho,
       EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid) AS em_uso
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname ~ '_ab_soft_reset$'
  AND EXISTS (
        SELECT 1
        FROM pg_class base
        JOIN pg_namespace basen ON basen.oid = base.relnamespace
        WHERE basen.nspname = n.nspname
          AND base.relkind = 'r'
          AND base.relname = regexp_replace(c.relname, '_ab_soft_reset$', '')
      )
ORDER BY pg_total_relation_size(c.oid) DESC;

-- 5b. EXECUÇÃO:
DO $$
DECLARE
  r        record;
  dropped  int := 0;
  skipped  int := 0;
BEGIN
  SET lock_timeout = '5s';

  FOR r IN
    SELECT n.nspname, c.relname,
           pg_total_relation_size(c.oid) AS bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname = 'airbyte_etl'
      AND c.relname ~ '_ab_soft_reset$'
      AND EXISTS (
            SELECT 1
            FROM pg_class base
            JOIN pg_namespace basen ON basen.oid = base.relnamespace
            WHERE basen.nspname = n.nspname
              AND base.relkind = 'r'
              AND base.relname = regexp_replace(c.relname, '_ab_soft_reset$', '')
          )
      AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid)
  LOOP
    BEGIN
      EXECUTE format('DROP TABLE IF EXISTS %I.%I', r.nspname, r.relname);
      dropped := dropped + 1;
      RAISE NOTICE 'DROP ok   %.% (%)', r.nspname, r.relname, pg_size_pretty(r.bytes);
    EXCEPTION WHEN OTHERS THEN
      skipped := skipped + 1;
      RAISE NOTICE 'PULADA    %.% — em uso: %', r.nspname, r.relname, SQLERRM;
    END;
    COMMIT;
  END LOOP;

  RAISE NOTICE 'Concluído: % dropadas, % puladas.', dropped, skipped;
END
$$;
