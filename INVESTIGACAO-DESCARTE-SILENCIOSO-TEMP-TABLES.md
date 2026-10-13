# Investigação — Descarte silencioso de dados no destination-postgres

**Data:** 2026-09-24
**Fork:** `crewhu/airbyte`, `destination-postgres`
**Plataforma:** Airbyte 1.8.1 (`container-orchestrator:1.8.1`)
**Branch:** `diagnostics/temp_table_worker_job_id` · PR [#12](https://github.com/crewhu/airbyte/pull/12)
**Status:** causa raiz identificada, correção validada em produção, **não compilada localmente**

## Resumo executivo

Syncs que reportavam `status: completed`, `Failures: []` e `recordsCommitted` igual ao
total lido **não gravavam nenhum registro na tabela final**. Os dados eram inseridos na
temp table e descartados no teardown.

| Job | Registros | Destino real |
|---|---|---|
| 41711 | 517 | descartados |
| 415061 | 1737 | descartados |
| 415108 | 2473 | descartados |
| 415109 | 2472 | descartados |
| **415238** | **272** | **gravados** ✅ (com a correção) |

A causa é upstream, no CDK, e **não** tem relação com a hipótese original de temp tables
órfãs: a plataforma 1.8.1 não encaminha as mensagens `TRACE STREAM_STATUS` para o stdin
do destination. Sem elas, o `StreamCompletionTracker` nunca marca nenhum stream como
completo, `allStreamsComplete()` retorna `false` num sync perfeitamente saudável, e a
finalização descarta todas as temp tables em vez de fazer o upsert.

## A cadeia do descarte

Quatro passos, todos em código do upstream:

| # | Arquivo:linha | O que acontece |
|---|---|---|
| 1 | `StreamCompletionTracker.kt:28` | `allStreamsComplete()` = `completedStreams.containsAll(expectedStreams)` → `false`, pois `completedStreams` está vazio |
| 2 | `DestinationLifecycle.kt:115` | `it.teardown(completionTracker.allStreamsComplete())` passa `false` |
| 3 | `StreamLoader.kt:34` | Com `completedSuccessfully = false`, fabrica um `StreamProcessingFailed` sintético |
| 4 | `DirectLoadTableStreamLoader.kt:110` | `streamFailure != null` → descarta a temp table sem upsert |

O `accept()` (`StreamCompletionTracker.kt:27`) é o **único** escritor de
`completedStreams`, e só é chamado a partir de uma `DestinationRecordStreamComplete`
(`DataFlowPipelineInputFlow.kt:53`). Sem trace, não há outro caminho.

## Como chegamos aqui — e o que foi descartado

A investigação passou por três hipóteses. As duas primeiras estavam **erradas**; ficam
registradas para que ninguém as repita.

### ❌ Hipótese 1 — mismatch de nomes prefixados

**Ideia:** o catálogo espera `auto_task_Tickets` (prefixado) e a trace traz `Tickets`
(nome da origem), então o `containsAll` compararia conjuntos que nunca se cruzam.

**Refutada por:** `DestinationMessageFactory.kt:114` aplica `namespaceMapper.map()` antes
do `catalog.getStream()`, e o catálogo é indexado por `mappedDescriptor`. Os dois lados já
chegam mapeados.

**Prova no log:** o diagnóstico do tracker imprimiu `unexpected=[]` — se fosse mismatch,
os nomes de origem apareceriam nessa lista.

### ❌ Hipótese 2 — `NamespaceMapper` sem `streamPrefix` em STDIO

**Ideia:** `DataChannelBeanFactory.kt:311` constrói `NamespaceMapper(SOURCE)` sem
`streamPrefix`, logo o mapper é identidade e não aplicaria o prefixo.

**Refutada por:** é verdade que o mapper é identidade em STDIO, mas o prefixo **já vem no
catálogo configurado** — o log mostra `unmappedName=auto_task_Tickets`. Ou seja,
`unmappedDescriptor == mappedDescriptor` e não há divergência a corrigir.

> Chegou a ser escrito um índice `byUnmappedDescriptor` no `DestinationCatalog` como
> fallback; foi **revertido** por ser idêntico ao índice primário, portanto inócuo.

### ✅ Hipótese 3 — as traces não chegam (confirmada)

O commit de diagnóstico `c0a4e19b` instrumentou a entrada do
`DestinationMessageFactory`, logando **toda** TRACE antes de qualquer lookup. O job 415109
rodou com esse build e produziu **zero linhas `trace-in`**.

Todo `Stream status TRACE received` que aparece no log do job vem do
`replication-orchestrator`, não do processo do destination. As mensagens nunca cruzam o
stdin.

## A correção

`StreamCompletionTracker.acceptEndOfInput()`, chamado de `DestinationLifecycle.run()`
**apenas no caminho de sucesso**:

```kotlin
// DestinationLifecycle.run(), após pipeline.run() retornar sem lançar
completionTracker.acceptEndOfInput()

finalizeIndividualStreams(streamLoaders)
teardownDestination()
```

O raciocínio: drenar o input e completar o pipeline **é** a prova de que a origem enviou
tudo que tinha — é a saída da própria source que fecha o stream.

**Comportamento de falha inalterado.** Um pipeline que lança continua no `catch`, continua
finalizando com os streams incompletos e continua descartando todas as temp tables. Se a
plataforma voltar a encaminhar traces, `accept()` preenche o conjunto antes e o
`acceptEndOfInput()` vira no-op.

## Validação em produção — job 415238

```
17:16:33 acceptEndOfInput: marking 6 stream(s) complete without a trace:
         [auto_task_Tickets, auto_task_TimeEntries, auto_task_Employees,
          auto_task_DailyAvailabilities, auto_task_Ticket_Entity_Information,
          auto_task_Holidays]
```

| Comportamento | Antes (415109) | Depois (415238) |
|---|---|---|
| Finalização | `did not complete; discarding temp table without upserting` | `ensureSchemaMatches` → `WITH deduped_source … UPDATE/INSERT INTO auto_task_*` |
| `DROP TABLE` | sem upsert | **depois** do upsert |
| Ocorrências de `discarding` | 6 | **0** |

**Confirmação independente — o cursor avançou.** O stream `Tickets` leu **21** registros,
contra 2231/2232 nos jobs anteriores. Nos jobs quebrados o cursor não se movia e a origem
relia a mesma janela a cada execução; a queda para 21 só é possível se a execução anterior
tiver de fato persistido.

## Efeito colateral positivo — `WORKER_JOB_ID`

A investigação começou por outro caminho: adicionar `WORKER_JOB_ID` à cadeia de resolução
do `uniqueId` da temp table (`DestinationStreamFactory.kt:96-101`). Os logs de produção
revelaram que `syncId=0` e que `CONNECTION_ID`/`AIRBYTE_CONNECTION_ID` estão ambos
`<absent>` — ou seja, **a cadeia caía no fallback aleatório em toda execução**, exatamente
o que o commit de diagnóstico `11d7aacdb49` suspeitava.

Com a mudança, `uniqueId=415238 source=WORKER_JOB_ID env`. Isso torna o nome da temp table
estável entre *attempts* do mesmo job e reduz retrabalho intra-job.

**Limite:** `WORKER_JOB_ID` **não** é estável entre jobs distintos, então não serve como
correção de integridade — um job novo continua não reclamando a temp table de um job falho.
Ver [HANDOFF-job-agnostic-temp-tables.md](https://github.com/crewhu/airbyte) no repo
`crewhu-trends-api`.

## Pendências

| Item | Detalhe |
|---|---|
| **Compilação** | Nada foi compilado localmente. O build exige JVM ≥ 21 para o plugin `io.airbyte.gradle`, enquanto o Groovy do `buildSrc` (Gradle 8.14) não lê class files do Java 26; só há JDK 17 e 26 instalados. `brew install --cask temurin@21` destrava. **O CI precisa validar antes do merge.** |
| **Issue upstream** | A causa real é a plataforma não encaminhar `STREAM_STATUS`. Vale abrir issue no Airbyte. |
| **Limpeza de commits** | Os dois commits de diagnóstico (`c91f701f`, `c0a4e19b`) cumpriram seu papel; o `trace-in` loga uma linha por trace recebida e polui. Considerar squash. |
| **Premissa de perda de dados** | A investigação original (`HANDOFF-job-agnostic-temp-tables.md`) pedia validar se os ~305 mil registros em temp tables órfãs eram perda real. **Isso continua não verificado no banco.** |

## Limite conhecido da correção

A correção infere completude do fim do input. Há um cenário que ela não distingue: se a
**source** morrer no meio e fechar o stdout **sem** sinalizar erro, o destination tratará
como sucesso e gravará dados parciais.

Isso não é regressão — antes o comportamento era descartar tudo, sempre, inclusive nos
syncs saudáveis. Mas é o limite desta abordagem. A solução definitiva é a plataforma
encaminhar as `STREAM_STATUS` ao destination.

## Commits

| Commit | Conteúdo |
|---|---|
| `9782af72` | `WORKER_JOB_ID` na cadeia do `uniqueId` |
| `c91f701f` | Diagnóstico: log dos streams que nunca completaram |
| `c0a4e19b` | Diagnóstico: log das traces na entrada do factory |
| `2fbd2be5` | **Correção:** input drenado ⇒ streams completos |
