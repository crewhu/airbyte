#!/usr/bin/env bash
#
# Limpeza das tabelas temporárias órfãs do schema airbyte_etl
# (padrão <nome>_<hash 8 hex>, ex.: trends_tickets_9455993b).
#
# Contexto: ver INVESTIGACAO-STORAGE-AIRBYTEETL.md na raiz do repo.
#
# Uso:
#   Conexão via variáveis padrão do psql (PGHOST, PGPORT, PGUSER, PGPASSWORD)
#   ou via DATABASE_URL. O database alvo é airbyte_etl_v3.
#
#   ./scripts/cleanup-orphan-temp-tables.sh              # dry-run: só lista o que seria dropado
#   ./scripts/cleanup-orphan-temp-tables.sh --execute    # dropa de fato
#   MIN_SIZE_MB=10 ./scripts/cleanup-orphan-temp-tables.sh --execute
#
# Segurança:
#   - Só considera tabelas do schema airbyte_etl cujo nome termina em _[0-9a-f]{8}
#     E cujo nome-base (sem o sufixo) exista como tabela real no mesmo schema.
#     Ou seja: só apaga o que é comprovadamente uma cópia temporária de uma
#     tabela conhecida (ex.: trends_tickets_9455993b só entra porque
#     trends_tickets existe). Uma tabela real que por coincidência termine em
#     8 caracteres hexadecimais NÃO é apagada.
#   - Ignora tabelas com QUALQUER lock ativo (sync em andamento usando a temp table).
#   - lock_timeout de 5s por DROP: se um sync pegar a tabela no meio do caminho,
#     o DROP falha sozinho em vez de enfileirar e travar o job.
#   - Por padrão só pega tabelas >= 50 MB (183 tabelas, ~42 GB dos 62 GB).
#     Ajuste MIN_SIZE_MB para ampliar/restringir.

set -euo pipefail

MIN_SIZE_MB="${MIN_SIZE_MB:-50}"
DB="${PGDATABASE:-airbyte_etl_v3}"
EXECUTE=false
[[ "${1:-}" == "--execute" ]] && EXECUTE=true

PSQL=(psql --no-psqlrc --quiet --tuples-only --no-align --dbname "$DB")
if [[ -n "${DATABASE_URL:-}" ]]; then
  PSQL=(psql --no-psqlrc --quiet --tuples-only --no-align "$DATABASE_URL")
fi

CANDIDATES_SQL="
SELECT format('%I.%I', n.nspname, c.relname) AS fqtn,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname = 'airbyte_etl'
  AND c.relname ~ '_[0-9a-f]{8}\$'
  -- só é candidata se o nome-base (sem o sufixo hash) existir como tabela real:
  AND EXISTS (
    SELECT 1
    FROM pg_class base
    JOIN pg_namespace basen ON basen.oid = base.relnamespace
    WHERE basen.nspname = n.nspname
      AND base.relkind = 'r'
      AND base.relname = regexp_replace(c.relname, '_[0-9a-f]{8}\$', '')
  )
  AND pg_total_relation_size(c.oid) >= ${MIN_SIZE_MB} * 1024 * 1024
  AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.relation = c.oid)
ORDER BY pg_total_relation_size(c.oid) DESC;
"

echo ">> Buscando tabelas órfãs >= ${MIN_SIZE_MB} MB sem locks ativos..."
CANDIDATES="$("${PSQL[@]}" --field-separator '|' --command "$CANDIDATES_SQL")"

if [[ -z "$CANDIDATES" ]]; then
  echo "Nenhuma tabela candidata encontrada."
  exit 0
fi

TOTAL=$(wc -l <<<"$CANDIDATES" | tr -d ' ')
echo ">> ${TOTAL} tabelas candidatas:"
column -t -s '|' <<<"$CANDIDATES"
echo

if ! $EXECUTE; then
  echo "DRY-RUN: nada foi dropado. Rode com --execute para apagar as tabelas acima."
  exit 0
fi

read -r -p "Confirma o DROP de ${TOTAL} tabelas no banco ${DB}? (digite 'sim') " CONFIRM
[[ "$CONFIRM" == "sim" ]] || { echo "Abortado."; exit 1; }

DROPPED=0
FAILED=0
while IFS='|' read -r FQTN SIZE; do
  [[ -z "$FQTN" ]] && continue
  # Um DROP por transação: falha em uma tabela não afeta as demais.
  if "${PSQL[@]}" --command "SET lock_timeout = '5s'; DROP TABLE IF EXISTS ${FQTN};" >/dev/null 2>&1; then
    echo "DROP ok   ${FQTN} (${SIZE})"
    DROPPED=$((DROPPED + 1))
  else
    echo "DROP FAIL ${FQTN} (${SIZE}) — provavelmente em uso, pulando"
    FAILED=$((FAILED + 1))
  fi
done <<<"$CANDIDATES"

echo
echo ">> Concluído: ${DROPPED} dropadas, ${FAILED} puladas."
echo ">> O espaço é devolvido ao SO imediatamente após o DROP (sem necessidade de VACUUM)."
