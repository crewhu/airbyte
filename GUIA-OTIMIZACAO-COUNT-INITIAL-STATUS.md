# Guia: trocar `COUNT(*)` por `LIMIT 1` na fase de initial status

A fase `initializeDestination` do destination-postgres gasta de 45 s a 2,5 min contando linhas que são imediatamente descartadas. Este guia explica o que medir, o que mudar, o que **não** mudar, e como verificar.

**Público:** quem for implementar ou revisar a mudança no fork
**Pré-requisito:** [`INVESTIGACAO-FALHA-SYNC-475338.md`](./INVESTIGACAO-FALHA-SYNC-475338.md) — a investigação que originou este guia

---

## Resumo do achado

| | |
|---|---|
| **Sintoma** | 45 s–2,5 min em `initializeDestination`; alertas falsos de "connection leak" no HikariCP |
| **Causa** | `SELECT COUNT(*)` em 36 tabelas (final + temp de 18 streams); o número é descartado |
| **O que é preciso** | 3 estados: ausente / vazia / não-vazia |
| **Correção** | `SELECT 1 FROM <t> LIMIT 1` |
| **Ganho** | custo estimado 536.016 → 0,26 (**~2.000.000×**), e passa a ser constante |

## Por que o count é desperdício

`DatabaseInitialStatusGatherer.kt:57` conta e joga fora:

```kotlin
val numberOfRecords: Long? = tableOperationsClient.countTable(tableName)
return when (numberOfRecords) {
    null -> null
    0L   -> DirectLoadTableStatus(isEmpty = true)
    else -> DirectLoadTableStatus(isEmpty = false)
}
```

O tipo de destino carrega só um booleano (`DirectLoadInitialStatus.kt:14`):

```kotlin
data class DirectLoadTableStatus(val isEmpty: Boolean)
```

Os 4 consumidores em `DirectLoadTableStreamLoader.kt` (linhas 188, 228, 314, 378) só testam `isEmpty`. Nenhum lê a contagem.

## Evidência de custo (prod, `airbyteetl`)

```
COUNT(*)  → Aggregate → Gather → Index Only Scan on trends_tickets [Rows: 2841975]
            Cost: 536016.36

LIMIT 1   → Limit → Seq Scan on trends_tickets
            Cost: 0.26
```

O `LIMIT 1` não cresce com a tabela; o `COUNT(*)` cresce linearmente.

---

## A mudança

### 1. `TableOperationsClient.kt` (CDK) — contrato novo com default compatível

```kotlin
/**
 * Returns whether the table is empty, or null if the table doesn't exist.
 *
 * Callers only need three states (missing / empty / non-empty), so implementations
 * should prefer a bounded existence probe over a full row count. The default derives
 * the answer from [countTable] so existing destinations keep working unchanged.
 */
suspend fun tableIsEmpty(tableName: TableName): Boolean? =
    countTable(tableName)?.let { it == 0L }
```

> O default é o que mantém bigquery, snowflake e os demais destinations funcionando sem alteração.

### 2. `PostgresDirectLoadSqlGenerator.kt` — SQL limitado

```kotlin
fun tableIsNonEmpty(tableName: TableName): String =
    "SELECT 1 FROM ${getFullyQualifiedName(tableName)} LIMIT 1;"
```

Mesmo idioma do `getGenerationId`, que já existe no arquivo — não é um padrão novo.

### 3. `PostgresAirbyteClient.kt` — override

```kotlin
override suspend fun tableIsEmpty(tableName: TableName): Boolean? =
    try {
        executeQuery(sqlGenerator.tableIsNonEmpty(tableName)) { resultSet -> !resultSet.next() }
    } catch (e: Exception) {
        log.debug(e) {
            "Table ${tableName.namespace}.${tableName.name} does not exist. Returning null to signal a missing table."
        }
        null
    }
```

Semântica: sem linha → vazia (`true`); com linha → não-vazia (`false`); exceção → ausente (`null`). Idêntica à do `countTable` para os três casos.

### 4. `DatabaseInitialStatusGatherer.kt` — passa a usar o novo método

```kotlin
private suspend fun getTableStatus(tableName: TableName): DirectLoadTableStatus? =
    tableOperationsClient.tableIsEmpty(tableName)?.let { DirectLoadTableStatus(isEmpty = it) }
```

---

## ⚠️ O que NÃO mudar

**Não altere a semântica de `countTable`.** Existe um segundo chamador que precisa do número exato — `PostgresOssChecker.kt:108`:

```kotlin
val tableCount = postgresAirbyteClient.countTable(qualifiedTableName)
require(tableCount == 1L) { "Failed to insert expected rows into check table. Actual written: $tableCount" }
```

É o `check` do connector, que insere 1 linha e confirma que 1 linha chegou. Transformar `countTable` em teste de existência quebra essa verificação silenciosamente — ela passaria a aceitar qualquer quantidade ≥ 1.

Por isso a mudança **adiciona** um método em vez de reaproveitar o existente.

## Alternativas descartadas

| Opção | Por que não |
|---|---|
| `pg_class.reltuples` | Estimativa. Vale `-1` em tabela nunca analisada e fica defasada após bulk load. Um "vazia" errado faz o loader escolher o caminho de merge errado — risco de dados por ganho nenhum frente ao `LIMIT 1`, que é exato e já é barato |
| `SELECT EXISTS(SELECT 1 FROM t)` | Custo equivalente; `LIMIT 1` escolhido por já ser o idioma do arquivo |
| Subir `leakDetectionThreshold` do Hikari | Esconde o alerta e mantém os 2,5 min |
| Rodar os counts em paralelo | Já são paralelos (`coroutineScope` + `launch` por stream); o gargalo é cada query |

---

## Como verificar

### Testes unitários

```bash
./gradlew :airbyte-integrations:connectors:destination-postgres:test
./gradlew :airbyte-cdk:bulk:core:load:test
```

Cobrir os três estados em `PostgresAirbyteClientTest`:

| Caso | Mock | Esperado |
|---|---|---|
| Tabela com linhas | `resultSet.next()` → `true` | `false` (não-vazia) |
| Tabela vazia | `resultSet.next()` → `false` | `true` (vazia) |
| Tabela ausente | `executeQuery` lança `SQLException` | `null` |

E o SQL em `PostgresDirectLoadSqlGeneratorTest`:

```kotlin
assertEquals("SELECT 1 FROM \"namespace\".\"name\" LIMIT 1;", sql)
```

### Verificação em runtime

No log do próximo sync, a fase de init deve cair de minutos para segundos:

```
INFO DestinationLifecycle$initializeDestination  Initializing the destination
...
INFO DestinationLifecycle$initializeDestination  Destination initialized
```

E devem **sumir**:

```
WARN ProxyLeakTask  Apparent connection leak detected
  at PostgresAirbyteClient.countTable(...)
```

Deve aparecer, no lugar dos `SELECT COUNT(*)`:

```
INFO PostgresAirbyteClient(executeQuery)  SELECT 1 FROM "airbyte_etl"."trends_tickets" LIMIT 1;
```

### Correção de comportamento (regressão)

A mudança é só de performance — o comportamento precisa ficar idêntico. Confirmar num sync real:

- Tabela final **nova** → criada normalmente (caminho `realTable == null`).
- Tabela final **populada** → dedupe/merge acontece como antes (caminho `isEmpty == false`).
- Temp table **remanescente de job anterior** → `getGenerationId` é consultado como antes (caminho `isEmpty == false`).

> O risco da mudança concentra-se em classificar errado "vazia" vs "não-vazia". Como o `LIMIT 1` é exato (não estimativa), esse risco é o mesmo do `COUNT(*)`.

## Escopo e limites

- **Isto não corrige o job 475338.** A falha ali é SIGTERM na source (provável OOMKill) — ver a investigação. Esta mudança reduz a janela de buffering da source, o que ajuda de lado, mas o limite de memória continua precisando subir se o OOMKill for confirmado.
- Os números de custo são **estimativas do planner**; `EXPLAIN ANALYZE` em produção foi bloqueado por permissão.
- A mudança afeta todos os destinations que herdam `BaseDirectLoadInitialStatusGatherer`, mas apenas o postgres ganha o caminho rápido — os outros caem no default derivado de `countTable`.
