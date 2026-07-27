# Build & Push da imagem Docker — destination-postgres (amd64)

Runbook para gerar a imagem Docker do connector `destination-postgres` na arquitetura
`linux/amd64` e publicá-la no Docker Hub.

> Contexto: o host de build é um macOS Apple Silicon (arm64) usando OrbStack como engine
> Docker. Como o destino é uma imagem `amd64`, o build é cross-platform via `docker buildx`
> (emulação fornecida pelo OrbStack/QEMU).

---

## 1. Pré-requisitos

| Requisito | Detalhe |
|---|---|
| Docker engine rodando | OrbStack **ou** Docker Desktop. Verifique com `docker version`. |
| buildx disponível | `docker buildx version` (já vem com Docker moderno). |
| Login no Docker Hub | `docker login` (necessário só para o push). |
| JDK + Gradle wrapper | Já presentes no repo (`./gradlew`). |

### Engine parado? (OrbStack)

Se o build falhar com algo como:

```
ERROR: failed to connect to the docker API at unix:///Users/<user>/.orbstack/run/docker.sock
```

o daemon não está de pé. Inicie e aguarde subir:

```bash
open -a OrbStack
# aguardar o daemon responder
until docker version --format '{{.Server.Os}}/{{.Server.Arch}}' >/dev/null 2>&1; do sleep 2; done
docker version --format 'Server {{.Server.Os}}/{{.Server.Arch}}'
```

Confira também qual context está ativo:

```bash
docker context ls
docker context show   # deve apontar para o engine que está rodando (ex.: orbstack)
```

---

## 2. Como o build é configurado

O connector usa o plugin Gradle `io.airbyte.gradle.docker` + a convenção local
[`airbyte-connector-docker-convention.gradle`](../../../buildSrc/src/main/groovy/airbyte-connector-docker-convention.gradle),
que lê parâmetros do [`metadata.yaml`](metadata.yaml):

| Origem | Valor atual |
|---|---|
| `dockerRepository` → nome da imagem | `airbyte/destination-postgres-vini` |
| `dockerImageTag` → tag | `3.0.5` (mas o `dockerBuildx` taggeia como `dev` por padrão) |
| `baseImage` (`connectorBuildOptions`) | `docker.io/airbyte/java-connector-base:2.0.1@sha256:ec89bd1a…` |
| Dockerfile | [`docker-images/Dockerfile.java-connector-non-airbyte-ci`](../../../docker-images/Dockerfile.java-connector-non-airbyte-ci) (compartilhado) |

Defaults relevantes da task `dockerBuildx`:

- `--platform` default = `linux/amd64,linux/arm64` (multi-arch) → **sobrescreva** com
  `-Pdocker.platform=linux/amd64` para gerar só amd64.
- `--output type=docker` → a imagem é carregada (`--load`) no daemon local.

---

## 3. Build da imagem `linux/amd64`

A partir da **raiz do repositório**:

```bash
./gradlew :airbyte-integrations:connectors:destination-postgres:dockerBuildx \
    -Pdocker.platform=linux/amd64 \
    --no-daemon
```

O pipeline executa, em ordem:

1. `compileKotlin` / `jar` — compila o connector.
2. `distTar` — gera `build/distributions/airbyte-app.tar` (aplicação Java/Kotlin).
3. `dockerCopyDistribution` — copia o `.tar` para `build/airbyte/docker/`.
4. `dockerCopyDockerfile` — copia o Dockerfile compartilhado.
5. `generateConnectorDockerBuildArgs` — gera `build/docker/buildArgs.properties`
   (`BASE_IMAGE`, `CONNECTOR_NAME`).
6. `dockerBuildx` — roda o equivalente a:

   ```bash
   docker buildx build \
       --label io.airbyte.app=destination-postgres \
       --label io.airbyte.version=dev \
       --build-arg BASE_IMAGE=docker.io/airbyte/java-connector-base:2.0.1@sha256:ec89bd1a… \
       --build-arg CONNECTOR_NAME=destination-postgres \
       --tag airbyte/destination-postgres-vini:dev \
       --file docker-images/Dockerfile.java-connector-non-airbyte-ci \
       --output type=docker \
       --platform linux/amd64 \
       airbyte-integrations/connectors/destination-postgres/build/airbyte/docker
   ```

Resultado: imagem local `airbyte/destination-postgres-vini:dev` (arquitetura amd64).

### Validação da arquitetura

```bash
docker image inspect airbyte/destination-postgres-vini:dev --format '{{.Os}}/{{.Architecture}}'
# esperado: linux/amd64
```

Smoke test opcional (executa o `spec` do connector):

```bash
docker run --rm --platform linux/amd64 airbyte/destination-postgres-vini:dev spec
```

---

## 4. Push para o Docker Hub

Repositório de destino: <https://hub.docker.com/r/vsantos98/destination-postgres>

```bash
# 1) re-tag da imagem local para o repositório de destino
docker tag airbyte/destination-postgres-vini:dev vsantos98/destination-postgres:dev

# 2) login (se ainda não estiver logado)
docker login

# 3) push
docker push vsantos98/destination-postgres:dev
```

### Confirmação do que foi publicado

```bash
docker buildx imagetools inspect vsantos98/destination-postgres:dev
```

A imagem é **single-arch (amd64)**, então o manifest é um
`application/vnd.docker.distribution.manifest.v2+json` (não um manifest list). O digest
publicado é idêntico ao da imagem amd64 local — essa é a confirmação de que a arquitetura
está correta.

---

## 5. Troca de tag / versão

A tag aqui é arbitrária (`dev`). Para publicar com outra tag **sem rebuildar**:

```bash
docker tag airbyte/destination-postgres-vini:dev vsantos98/destination-postgres:<nova-tag>
docker push vsantos98/destination-postgres:<nova-tag>
```

Alternativa: editar `dockerRepository` / `dockerImageTag` no `metadata.yaml` antes do passo 3
para que o nome/tag já saiam corretos do build.

---

## 6. Referência rápida (TL;DR)

```bash
# garantir engine de pé
open -a OrbStack
until docker version >/dev/null 2>&1; do sleep 2; done

# build amd64
./gradlew :airbyte-integrations:connectors:destination-postgres:dockerBuildx \
    -Pdocker.platform=linux/amd64 --no-daemon

# validar
docker image inspect airbyte/destination-postgres-vini:dev --format '{{.Os}}/{{.Architecture}}'

# tag + push
docker tag airbyte/destination-postgres-vini:dev vsantos98/destination-postgres:dev
docker push vsantos98/destination-postgres:dev
```

---

## 7. Histórico desta build

| Campo | Valor |
|---|---|
| Imagem publicada | `vsantos98/destination-postgres:dev` |
| Digest | `sha256:c7009f7b227622adfa5b0f77e6fa3301c8e93ec06d1ff8d6d8c201f146ad3d67` |
| Arquitetura | `linux/amd64` |
| Base image | `airbyte/java-connector-base:2.0.1` |
| Conteúdo notável | inclui o fix de *temp table name mismatch* (BUG-001) |
