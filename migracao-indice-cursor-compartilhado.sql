-- Remove o índice de cursor compartilhado deixado pelo esquema antigo.
--
-- Contexto: airbyte_etl.auto_task_TimeEntries é escrita por várias connections v3 com
-- cursors diferentes. O índice antigo era nomeado só pela tabela, então cada sync dropava
-- e recriava o do tenant anterior -- ~2 mil DROP/CREATE, com ACCESS EXCLUSIVE enfileirando
-- leituras (incidente observado: 44 sessões bloqueadas por 67s).
--
-- O connector corrigido cria um índice por cursor, CONCURRENTLY, e NUNCA dropa nada.
-- Este script faz a limpeza do índice antigo UMA vez, fora do caminho de sync.
--
-- PRÉ-REQUISITO: o connector corrigido já deve estar em produção, e cada cursor distinto
-- já deve ter criado o seu índice novo. Confira antes de rodar (passo 1).
--
-- Rodar manualmente, fora de transação (CONCURRENTLY não funciona dentro de BEGIN/COMMIT).
-- NÃO envolver em BEGIN/COMMIT. NÃO rodar via psql -1 / --single-transaction.

-- 1. Conferir quais índices de cursor existem hoje nesta tabela.
--    Esperado depois do deploy: o legado + um por cursor distinto.
SELECT indexname,
       indexdef,
       pg_size_pretty(pg_relation_size((quote_ident(schemaname) || '.' || quote_ident(indexname))::regclass)) AS size
FROM pg_indexes
WHERE schemaname = 'airbyte_etl'
  AND tablename  = 'auto_task_TimeEntries'
  AND indexname LIKE 'idx_cursor_%'
ORDER BY indexname;

-- 2. Conferir se há algum índice INVALID (resto de um CONCURRENTLY que falhou).
--    Se aparecer algum, dropar CONCURRENTLY também -- ele não é usado pelo planner.
SELECT c.relname AS index_name, i.indisvalid
FROM pg_index i
JOIN pg_class c ON c.oid = i.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'airbyte_etl'
  AND c.relname LIKE 'idx_cursor_auto_task_TimeEntries%'
  AND NOT i.indisvalid;

-- 3. Só então remover o legado. CONCURRENTLY não bloqueia leitura nem escrita.
--    Pode demorar; se falhar, pode ser repetido com segurança.
DROP INDEX CONCURRENTLY IF EXISTS "airbyte_etl"."idx_cursor_auto_task_TimeEntries";

-- 4. Confirmar o estado final: só os índices por cursor devem restar.
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'airbyte_etl'
  AND tablename  = 'auto_task_TimeEntries'
  AND indexname LIKE 'idx_cursor_%'
ORDER BY indexname;
