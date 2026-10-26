# Job 475338: a destination não falhou — a source foi morta (SIGTERM)

As 3 tentativas do job 475338 falharam com `exit code 143` na **source**. A destination-postgres aparece no stacktrace apenas como `Suppressed`: ela foi derrubada junto, como consequência. Separadamente, a investigação confirmou um gargalo real no fork: o `COUNT(*)` da fase de inicialização custa ~2 milhões de vezes mais que o necessário.

**Público:** time de dados / mantenedores do fork do destination-postgres
**Escopo:** análise dos logs do job 475338 (2026-10-23) + leitura de código + EXPLAIN em produção
**Connection:** `c42191a7-7e1f-4311-b6a9-15f3d3ead8f7` · **Build:** `diagnostics-temp_table_worker_job_id-2fbd2be5`

---

## Veredito

| Hipótese | Status |
|---|---|
| Erro de SQL / schema na destination | **Descartada** — nenhuma exceção de Postgres em nenhuma tentativa |
| Timeout de heartbeat | **Descartada** — ver cálculo abaixo |
| **Source morta por sinal externo (OOMKill provável)** | **Causa raiz — a confirmar no k8s** |
| `COUNT(*)` caro na inicialização | **Confirmado** — problema real, mas não é a causa da falha |

## A falha reportada

Idêntica nas 3 tentativas:

```
"failureOrigin" : "source",
"internalMessage" : "Source process exited with non-zero exit code 143",
```

`143 = 128 + 15` → **SIGTERM**. O processo foi **morto**; não quebrou sozinho. A exceção da destination vem aninhada como `Suppressed`:

```
Suppressed: DestinationException: Destination process exited with non-zero exit code 143
```

Mesmo sinal, mesmo instante: quando a source morre, o orchestrator derruba o pod inteiro.

### A destination chegou a concluir o pipeline

Na tentativa 2, antes de ser morta:

```
12:48:15 PipelineRunner(run):75 Destination Pipeline Completed — Successfully
12:48:15 StreamCompletionTracker  marking 18 stream(s) complete
12:48:15 Finalizing stream null:trends_boards   → merge + DROP temp  OK
12:48:16 Finalizing stream null:trends_calendars → merge + DROP temp  OK
12:48:16 Finalizing stream null:trends_companies → merge iniciado
12:48:17 DestinationReader error  ← morta no meio do finalize de trends_employees
```

Ou seja: a destination processou tudo, começou o merge temp → final e foi interrompida **2 segundos** depois de iniciar a finalização. Isso é um kill externo, não um defeito dela.

## Onde cada tentativa morreu

| Attempt | Morte | Stream em leitura | Lido até ali |
|---|---|---|---|
| 1 | 12:26:21 | `tickets` | 130.000 reg / 550 MB |
| 2 | 12:48:14 | `projects_contacts` | 269.011 reg / **1,4 GB** |
| 3 | (log truncado) | — | — |

A tentativa 2 foi **mais longe** que a 1 e mesmo assim morreu. O ponto de morte varia com o volume acumulado, não com um stream específico — assinatura de pressão de memória, não de registro defeituoso.

### Por que não é heartbeat

```
Starting workload heartbeat (interval=10s; timeout=600s)
```

Na tentativa 2 houve atividade contínua até 12:47:36 e a morte às 12:48:14 — **38 s** depois. O timeout é de 600 s. Não bate.

### Por que OOMKill é a hipótese forte

- A source roda com `limits={memory=2Gi}`.
- É `source-declarative-manifest:6.61.6` (low-code CDK), que acumula memória em streams grandes.
- `tickets` sozinho emitiu **1,1 GB** / 178.116 registros.
- **Nenhuma mensagem de erro da source antes do SIGTERM** — exatamente o que acontece num OOMKill: o kernel mata o processo e o app não loga nada.

## Como confirmar

O log do job não registra o motivo do kill. A evidência está no Kubernetes:

```bash
kubectl describe pod replication-job-475338-attempt-2 -n <ns>
# procurar no container "source":
#   Last State: Terminated
#   Reason: OOMKilled        ← confirmação
#   Exit Code: 137 ou 143

kubectl get events -n <ns> --sort-by=.lastTimestamp | grep -iE 'oom|evict|475338'
```

> `Reason: OOMKilled` fecha o caso. Se vier `Error` sem OOM, investigar evicção do node ou preempção de spot.

---

# Investigação: o custo do `COUNT(*)` na inicialização

Problema independente da falha, mas real e mensurável.

## O sintoma nos logs

A fase `initializeDestination` roda `SELECT COUNT(*)` na tabela final **e** na temp de cada um dos 18 streams — 36 counts por tentativa:

| Attempt | Duração da fase |
|---|---|
| 1 | 12:14:34 → 12:17:10 = **2 min 36 s** |
| 2 | 12:28:48 → 12:29:54 = **1 min 06 s** |
| 3 | 12:49:53 → 12:50:38 = **45 s** |

Dois alertas do HikariCP durante a fase:

```
WARN ProxyLeakTask  Apparent connection leak detected
  at PostgresAirbyteClient.countTable(PostgresAirbyteClient.kt:51)
  at BaseDirectLoadInitialStatusGatherer.getTableStatus(DatabaseInitialStatusGatherer.kt:57)
```

**Não é vazamento.** O próprio log desmente logo depois:

```
INFO ProxyLeakTask(cancel) Previously reported leaked connection ... was returned to the pool (unleaked)
```

É um `COUNT(*)` que demora mais que o `leakDetectionThreshold` do Hikari. Os counts de `trends_tickets` e `trends_time_entries` levam **40–45 s cada**. O alarme é um sintoma do custo da query, não um bug de pool.

## Causa raiz: o count é jogado fora

O valor exato **nunca é usado**. Em `DatabaseInitialStatusGatherer.kt:57-65`:

```kotlin
val numberOfRecords: Long? = tableOperationsClient.countTable(tableName)
return when (numberOfRecords) {
    null -> null                                  // tabela não existe
    0L   -> DirectLoadTableStatus(isEmpty = true) // vazia
    else -> DirectLoadTableStatus(isEmpty = false)// tem dados
}
```

E o tipo de destino carrega **só um booleano** (`DirectLoadInitialStatus.kt:14`):

```kotlin
data class DirectLoadTableStatus(
    val isEmpty: Boolean,
)
```

Ou seja: contam-se **2,8 milhões de linhas** para decidir `isEmpty == true/false`.

Todos os consumidores tratam o campo como booleano puro — nenhum lê a contagem (`DirectLoadTableStreamLoader.kt`, linhas 188, 228, 314, 378):

```kotlin
if (initialStatus.tempTable.isEmpty) { ... }
initialStatus.realTable.isEmpty || tableOperationsClient.getGenerationId(...) >= ...
```

> São necessários **três estados** — ausente / vazia / não-vazia — e nada além disso.

## Medição em produção (EXPLAIN, banco `airbyteetl`)

```sql
-- query atual
SELECT COUNT(*) AS "total" FROM "airbyte_etl"."trends_tickets";
```
```
→ Aggregate (Cost: 536016.36..536016.37)
  → Gather (Cost: 536016.14..536016.35)
    → Aggregate
      → Index Only Scan on trends_tickets [Rows: 2841975]
```

```sql
-- alternativa proposta
SELECT 1 FROM "airbyte_etl"."trends_tickets" LIMIT 1;
```
```
→ Limit (Cost: 0.00..0.26)
  → Seq Scan on trends_tickets
```

| | Plano | Custo estimado |
|---|---|---|
| `COUNT(*)` | varre **2,8 M linhas** | **536.016** |
| `LIMIT 1` | para na **primeira linha** | **0,26** |

**≈ 2.000.000× mais barato.** E o custo do `LIMIT 1` é **constante**: não cresce com a tabela. O do `COUNT(*)` cresce linearmente — vai piorar a cada sync.

## Correção proposta

Adicionar um método de três estados ao lado do `countTable` (sem alterá-lo — ver "cuidado" abaixo).

**`PostgresDirectLoadSqlGenerator.kt`** — mesmo idioma do `getGenerationId`, que já existe no arquivo:

```kotlin
fun tableIsNonEmpty(tableName: TableName): String =
    "SELECT 1 FROM ${getFullyQualifiedName(tableName)} LIMIT 1;"
```

**`PostgresAirbyteClient.kt`** — `null` = ausente, `true/false` = vazia/não-vazia:

```kotlin
override suspend fun tableIsEmpty(tableName: TableName): Boolean? =
    try {
        executeQuery(sqlGenerator.tableIsNonEmpty(tableName)) { rs -> !rs.next() }
    } catch (e: Exception) {
        log.debug(e) { "Table ${tableName.namespace}.${tableName.name} does not exist." }
        null
    }
```

**`TableOperationsClient.kt`** — default que preserva compatibilidade com os outros destinations:

```kotlin
/** null = tabela ausente; senão, se está vazia. Default: deriva de countTable. */
suspend fun tableIsEmpty(tableName: TableName): Boolean? =
    countTable(tableName)?.let { it == 0L }
```

**`DatabaseInitialStatusGatherer.kt`** — passa a consultar o novo método:

```kotlin
private suspend fun getTableStatus(tableName: TableName): DirectLoadTableStatus? =
    tableOperationsClient.tableIsEmpty(tableName)?.let { DirectLoadTableStatus(isEmpty = it) }
```

### Cuidado: não alterar `countTable` diretamente

Existe um **segundo chamador** que precisa do número exato — `PostgresOssChecker.kt:108`:

```kotlin
val tableCount = postgresAirbyteClient.countTable(qualifiedTableName)
require(tableCount == 1L) { "Failed to insert expected rows into check table. Actual written: $tableCount" }
```

Trocar a semântica de `countTable` quebraria o `check` do connector. Daí a adição de um método novo em vez de mudar o existente.

### Alternativas descartadas

| Opção | Por que não |
|---|---|
| `pg_class.reltuples` | Estimativa: vale `-1` em tabela nunca analisada e fica defasada após bulk load. Decidir "vazia" errado faz o loader escolher o caminho de merge errado — risco de corromper dados por ganho nenhum frente ao `LIMIT 1`, que é exato e já é barato |
| `SELECT EXISTS(SELECT 1 FROM t)` | Equivalente em custo ao `LIMIT 1`; `LIMIT 1` foi escolhido por já ser o idioma do arquivo (`getGenerationId`) |
| Subir `leakDetectionThreshold` | Esconde o sintoma e mantém os 2,5 min |

## Impacto esperado

- Elimina os ~45 s–2,5 min de inicialização por tentativa.
- Remove os falsos alarmes de "connection leak".
- Encurta a janela em que a source produz sem consumo correspondente — o que **reduz a pressão de memória da source**, atacando de lado a causa raiz provável do SIGTERM.

> Mesmo assim: isto é mitigação. Se o OOMKill for confirmado, o limite de memória da source precisa subir de qualquer forma.

---

## Recomendações

| # | Ação | Prazo |
|---|---|---|
| 1 | `kubectl describe pod` → confirmar `OOMKilled` | Imediato |
| 2 | Se confirmado: subir limite de memória da source 2Gi → 4Gi | Imediato |
| 3 | Aplicar a troca `COUNT(*)` → `LIMIT 1` no fork | Próximo build |
| 4 | Avaliar separar `tickets` (1,1 GB / 178k reg) em connection própria | Backlog |

## Limites conhecidos

- **O OOMKill não está confirmado** — é a hipótese mais compatível com as evidências (ausência de erro da source, morte proporcional ao volume, heartbeat descartado), mas só o `kubectl describe` fecha o diagnóstico.
- Os custos do EXPLAIN são **estimativas do planner** (`ANALYZE` real foi bloqueado por permissão no MCP de produção). A ordem de grandeza é inequívoca, mas o tempo real não foi cronometrado — os 40–45 s vêm dos timestamps do log do job.
- `reltuples` do `trends_tickets` indicou ~2,8 M no plano do `COUNT(*)` e ~6,8 M no do `LIMIT 1` (estimativas de caminhos diferentes); a contagem real não foi medida.
- O log da tentativa 3 está truncado — assumiu-se o mesmo padrão das anteriores.
- A correção proposta **não foi implementada nem testada** neste documento.

## Referências

| Recurso | Uso |
|---|---|
| `DatabaseInitialStatusGatherer.kt:57-65` | Count descartado em favor de booleano |
| `DirectLoadInitialStatus.kt:14` | `DirectLoadTableStatus` só tem `isEmpty` |
| `DirectLoadTableStreamLoader.kt:188,228,314,378` | Consumidores tratam como booleano |
| `PostgresAirbyteClient.kt:49-62` | `countTable` + ponto do alerta de leak |
| `PostgresDirectLoadSqlGenerator.kt:544` | SQL do `COUNT(*)` |
| `PostgresOssChecker.kt:108` | Segundo chamador — exige count exato |
| EXPLAIN em `airbyteetl` (prod) | Comparação de custo dos planos |
