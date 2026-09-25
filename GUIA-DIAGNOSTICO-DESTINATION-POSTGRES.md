# Guia — Diagnosticar dados que não chegam na tabela final

**Contexto:** [INVESTIGACAO-DESCARTE-SILENCIOSO-TEMP-TABLES.md](./INVESTIGACAO-DESCARTE-SILENCIOSO-TEMP-TABLES.md)

Guia prático para o sintoma: *o sync diz que deu certo, mas a tabela final não recebeu
nada.*

## O sintoma

O job reporta sucesso e mesmo assim o banco não muda:

```
"status" : "completed",
"recordsCommitted" : 2472,
"Failures": [ ]
```

E no log do destination:

```
WARN  One or more streams did not complete. Skipping destructive finalization operations...
WARN  Stream <nome> did not complete; discarding temp table <temp> without upserting
INFO  DROP TABLE IF EXISTS "<schema>"."<temp>";
```

> ⚠️ A contagem `recordsCommitted` da plataforma **não** significa que o dado chegou na
> tabela final. Ela conta o que foi entregue ao destination, não o que foi persistido.

## Triagem rápida

Grep no log do job, nesta ordem:

| # | Buscar | Interpretação |
|---|---|---|
| 1 | `discarding temp table` | Se aparece, é este problema. Siga. |
| 2 | `stream-completion` | Mostra `expected` / `completed` / `missing` / `unexpected` |
| 3 | `trace-in` | Mostra as TRACE que entram no destination |

### Lendo o `stream-completion`

| Resultado | Significado |
|---|---|
| `completed=[]` | As mensagens de complete nunca chegaram ao tracker → siga para o `trace-in` |
| `unexpected=[<nomes>]` | Mismatch de nomes — os dois lados usam espaços de nomes diferentes |
| `missing=[]` | Tracker OK; o problema é outro |

### Lendo o `trace-in`

| Resultado | Significado |
|---|---|
| **Nenhuma linha** | A plataforma não encaminha `STREAM_STATUS` ao destination — **é a causa conhecida** |
| Linhas + `lookup FAILED` | As traces chegam mas o catálogo não as resolve |

> Atenção: `Stream status TRACE received` vem do **replication-orchestrator**, não do
> destination. Não confunda com `trace-in`. A presença daquele não prova que o destination
> recebeu coisa alguma.

## Verificar qual build está rodando

Toda imagem do fork loga o commit na inicialização:

```
crewhu fork | destination-postgres | build=<tag> (<hash>)
```

Confira o hash antes de concluir qualquer coisa — mais de uma rodada desta investigação foi
gasta analisando log de build antigo.

## Identidade da temp table

```
crewhu fork | temp-table-identity | CONNECTION_ID=<absent> AIRBYTE_CONNECTION_ID=<absent> WORKER_JOB_ID=415238 syncId=0
crewhu fork | temp-table-identity | stream=… uniqueId=415238 source=WORKER_JOB_ID env (...)
```

A cadeia de resolução (`DestinationStreamFactory.kt:96-101`), em ordem:

| Degrau | Estabilidade |
|---|---|
| `CONNECTION_ID` | determinístico — **ausente** em produção |
| `AIRBYTE_CONNECTION_ID` | determinístico — **ausente** em produção |
| `WORKER_JOB_ID` | estável entre *attempts*, muda a cada job |
| `syncId` | muda a cada sync — **é `0` em produção**, nunca alcançado |
| fallback | UUID aleatório — órfãs irreclamáveis |

Se o `source=` indicar `workerFallbackUniqueId (RANDOM ...)`, as temp tables órfãs desse
job nunca serão reaproveitadas por ninguém.

## Sinal de que o cursor voltou a avançar

Compare a contagem lida do mesmo stream entre execuções consecutivas:

```
Read 2231 records from Tickets stream   ← job quebrado (relê a mesma janela)
Read 21 records from Tickets stream     ← job corrigido (cursor avançou)
```

Uma queda brusca é confirmação independente de que a execução anterior persistiu de
verdade.

## Build local

O build está bloqueado por conflito de toolchain **pré-existente**:

- o plugin `io.airbyte.gradle` exige **JVM ≥ 21**
- o Groovy do `buildSrc` (Gradle 8.14) não lê class files do **Java 26**

Com apenas JDK 17 e 26 instalados, nenhum funciona. Solução:

```bash
brew install --cask temurin@21
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home \
  ./gradlew :airbyte-cdk:bulk:core:bulk-cdk-core-load:compileKotlin
```

## Arquivos relevantes

| Arquivo | Papel |
|---|---|
| `airbyte-cdk/bulk/core/load/.../dataflow/finalization/StreamCompletionTracker.kt` | Decide se os streams completaram |
| `airbyte-cdk/bulk/core/load/.../dataflow/DestinationLifecycle.kt` | Orquestra init → pipeline → finalize → teardown |
| `airbyte-cdk/bulk/core/load/.../write/StreamLoader.kt` | `teardown()` converte "incompleto" em falha sintética |
| `airbyte-cdk/bulk/core/load/.../table/directload/DirectLoadTableStreamLoader.kt` | Faz upsert ou descarta a temp table |
| `airbyte-cdk/bulk/core/load/.../message/DestinationMessageFactory.kt` | Converte TRACE em mensagem de domínio |
| `airbyte-cdk/bulk/core/load/.../command/DestinationStreamFactory.kt` | Resolve o `uniqueId` da temp table |
