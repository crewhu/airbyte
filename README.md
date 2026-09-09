# Crewhu fork — focus: `destination-postgres`

> This is a fork of [airbytehq/airbyte](https://github.com/airbytehq/airbyte). The upstream
> README follows below and is unchanged.

The only connector actively worked on here is **`destination-postgres`**
([`airbyte-integrations/connectors/destination-postgres/`](airbyte-integrations/connectors/destination-postgres/)).
Everything else in this repo is upstream code we carry along but do not modify.

We build and publish our own image of this connector and point our Airbyte
deployment at it, so fixes can ship without waiting on an upstream release.

## Building the connector image

**The only requirement is a running Docker engine.** No JDK, no Gradle, no SDKMAN.

```bash
cd airbyte-integrations/connectors/destination-postgres

# build locally, run the unit tests, publish nothing
./build-and-push.sh --tag my-tag

# build and publish to the registry (needs `docker login` first)
./build-and-push.sh --tag my-tag --push

# fast iteration, tests skipped
./build-and-push.sh --tag wip --skip-tests
```

`./build-and-push.sh --help` lists every option.

### Publishing

Pushing needs a Docker Hub login, which is interactive:

```bash
docker login          # user: vsantos98
./build-and-push.sh --tag my-tag --push
```

The script prints the published digest and the image reference to paste into
Airbyte (destination → *Change docker image*):

```
vsantos98/destination-postgres:my-tag
```

**Use a new tag for each build.** Reusing a tag makes rollback impossible and
lets Airbyte serve a cached image instead of the one you just pushed. The
script warns and asks for confirmation before overwriting a tag that already
exists in the registry.

Before publishing, it verifies the image architecture and runs the connector's
`spec` command; if either fails it aborts rather than pushing a broken image.

### How the build works, and why

The Airbyte build needs **JDK 21 specifically** — the Gradle plugins reject
anything older, and Gradle 8.14 cannot read class files produced by Java 26+.
That made builds depend on whichever JDK a given machine happened to have.
[`Dockerfile.builder`](airbyte-integrations/connectors/destination-postgres/Dockerfile.builder)
pins that toolchain so the host's Java version is irrelevant.

The build runs in two stages:

1. **Compile in the container** — produces `build/distributions/airbyte-app.tar`.
2. **Assemble the image on the host** — `docker buildx` turns that tar into the
   connector image, cross-building for `linux/amd64`.

They are split on purpose. Building the image inside the container would need
Docker-in-Docker; this way the container never needs a daemon or a mounted
socket. The Gradle cache lives in a named volume (`airbyte-connector-gradle-cache`),
so only the first build pays the dependency download.

## Local changes to the connector

Both changes live on `feature/fix_temp_table_collision`.

**Unique temp table names.** Temp table names in the Direct Load (v3) path were
derived only from the stream name, so concurrent connections syncing the same
stream into the same schema dropped each other's temp tables mid-job. The name
hash now includes a connection-scoped id (`CONNECTION_ID` env var, `syncId`, or
a per-worker random fallback).

**No indexes on temp tables.** Every job was issuing three `CREATE INDEX`
statements (primary key, cursor, `_airbyte_extracted_at`) against the temp table
it had just created — taking a `SHARE` lock and making the subsequent `COPY` pay
index maintenance per row. None of those indexes can ever be used: the temp
table is read exactly once, in full, by the dedup CTE in `upsertTable`, which is
a `ROW_NUMBER()` window over the whole table with no `WHERE` clause. Postgres
plans that as a sequential scan plus a sort regardless of indexes. Indexes on
the real table are untouched.

---

<p align="center">
  <a href="https://airbyte.com"><img src="https://assets.website-files.com/605e01bc25f7e19a82e74788/624d9c4a375a55100be6b257_Airbyte_logo_color_dark.svg" alt="Airbyte"></a>
</p>
<p align="center">
    <em>Data integration platform for ELT pipelines from APIs, databases & files to databases, warehouses & lakes</em>
</p>
<p align="center">
<a href="https://github.com/airbytehq/airbyte/stargazers/" target="_blank">
    <img src="https://img.shields.io/github/stars/airbytehq/airbyte?style=social&label=Star&maxAge=2592000" alt="Test">
</a>
<a href="https://github.com/airbytehq/airbyte/releases" target="_blank">
    <img src="https://img.shields.io/github/v/release/airbytehq/airbyte?color=white" alt="Release">
</a>
<a href="https://airbytehq.slack.com/" target="_blank">
    <img src="https://img.shields.io/badge/slack-join-white.svg?logo=slack" alt="Slack">
</a>
<a href="https://www.youtube.com/c/AirbyteHQ/?sub_confirmation=1" target="_blank">
    <img alt="YouTube Channel Views" src="https://img.shields.io/youtube/channel/views/UCQ_JWEFzs1_INqdhIO3kmrw?style=social">
</a>
<a href="https://github.com/airbytehq/airbyte/actions/workflows/gradle.yml" target="_blank">
    <img src="https://img.shields.io/github/actions/workflow/status/airbytehq/airbyte/gradle.yml?branch=master" alt="Build">
</a>
<a href="https://github.com/airbytehq/airbyte/tree/master/docs/project-overview/licenses" target="_blank">
    <img src="https://img.shields.io/static/v1?label=license&message=MIT&color=white" alt="License">
</a>
<a href="https://github.com/airbytehq/airbyte/tree/master/docs/project-overview/licenses" target="_blank">
    <img src="https://img.shields.io/static/v1?label=license&message=ELv2&color=white" alt="License">
</a>
</p>

We believe that only an **open-source solution to data movement** can cover the long tail of data sources while empowering data engineers to customize existing connectors. Our ultimate vision is to help you move data from any source to any destination. Airbyte provides a [catalog](https://docs.airbyte.com/integrations/) of 600+ connectors for APIs, databases, data warehouses, and data lakes.

![Airbyte Connections UI](https://github.com/airbytehq/airbyte/assets/38087517/35b01d0b-00bf-407b-87e6-a5cd5cd720b5)
_Screenshot taken from [Airbyte Cloud](https://cloud.airbyte.com/signup)_.

### Getting Started

- [Deploy Airbyte Open Source](https://docs.airbyte.com/quickstart/deploy-airbyte) or set up [Airbyte Cloud](https://docs.airbyte.com/cloud/getting-started-with-airbyte-cloud) to start centralizing your data.
- Create connectors in minutes with our [no-code Connector Builder](https://docs.airbyte.com/connector-development/connector-builder-ui/overview) or [low-code CDK](https://docs.airbyte.com/connector-development/config-based/low-code-cdk-overview).
- Explore popular use cases in our [tutorials](https://airbyte.com/tutorials).
- Orchestrate Airbyte syncs with [Airflow](https://docs.airbyte.com/operator-guides/using-the-airflow-airbyte-operator), [Prefect](https://docs.airbyte.com/operator-guides/using-prefect-task), [Dagster](https://docs.airbyte.com/operator-guides/using-dagster-integration), [Kestra](https://docs.airbyte.com/operator-guides/using-kestra-plugin), or the [Airbyte API](https://reference.airbyte.com/).

Try it out yourself with our [demo app](https://demo.airbyte.io/), visit our [full documentation](https://docs.airbyte.com/), and learn more about [recent announcements](https://airbyte.com/blog-categories/company-updates). See our [registry](https://connectors.airbyte.com/files/generated_reports/connector_registry_report.html) for a full list of connectors already available in Airbyte or Airbyte Cloud.

### Join the Airbyte Community

The Airbyte community can be found in the [Airbyte Community Slack](https://airbyte.com/community), where you can ask questions and voice ideas. You can also ask for help in our [Airbyte Forum](https://github.com/airbytehq/airbyte/discussions). Airbyte's roadmap is publicly viewable on [GitHub](https://github.com/orgs/airbytehq/projects/37/views/1?pane=issue&itemId=26937554).

For videos and blogs on data engineering and building your data stack, check out Airbyte's [Content Hub](https://airbyte.com/content-hub), [YouTube](https://www.youtube.com/c/AirbyteHQ), and sign up for our [newsletter](https://airbyte.com/newsletter).

### Contributing

If you've found a problem with Airbyte, please open a [GitHub issue](https://github.com/airbytehq/airbyte/issues/new/choose). To contribute to Airbyte and see our Code of Conduct, please see the [contributing guide](https://docs.airbyte.com/contributing-to-airbyte/). We have a list of [good first issues](https://github.com/airbytehq/airbyte/labels/contributor-program) that contain bugs that have a relatively limited scope. This is a great place to get started, gain experience, and get familiar with our contribution process.

#### PR Permission Requirements

When submitting a pull request, please ensure that Airbyte maintainers have write access to your branch. This allows us to apply formatting fixes and dependency updates directly, significantly speeding up the review and approval process.

To enable write access on your PR from Airbyte maintainers, please check the "Allow edits from maintainers" box when submitting from your PR. You must also create your PR from a fork in your **personal GitHub account** rather than an organization account, or else you will not see this option. The requirement to create from your personal fork is based on GitHub's additional security restrictions for PRs created from organization forks. For more information about the GitHub security model, please see the [GitHub documentation page regarding PRs from forks](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/allowing-changes-to-a-pull-request-branch-created-from-a-fork).

For more details on contribution requirements, please see our [contribution workflow documentation](https://docs.airbyte.com/platform/contributing-to-airbyte#standard-contribution-workflow).

### Security

Airbyte takes security issues very seriously. **Please do not file GitHub issues or post on our public forum for security vulnerabilities**. Email `security@airbyte.io` if you believe you have uncovered a vulnerability. In the message, try to provide a description of the issue and ideally a way of reproducing it. The security team will get back to you as soon as possible.

[Airbyte Enterprise](https://airbyte.com/airbyte-enterprise) also offers additional security features (among others) on top of Airbyte open-source.

### License

See the [LICENSE](docs/LICENSE) file for licensing information, and our [FAQ](https://docs.airbyte.com/platform/developer-guides/licenses/license-faq) for any questions you may have on that topic.

### Thank You

Airbyte would not be possible without the support and assistance of other open-source tools and companies! Visit our [thank you page](THANK-YOU.md) to learn more about how we build Airbyte.

<a href="https://github.com/airbytehq/airbyte/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=airbytehq/airbyte"/>
</a>
