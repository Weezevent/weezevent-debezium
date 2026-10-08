# Weezevent fork of Debezium

This repository is Weezevent's fork of [debezium/debezium](https://github.com/debezium/debezium).
It carries a small set of Weezevent-specific patches on top of a pinned upstream release and is
built into the `kafka-connect` production image.

## Branch model

| Branch | Role | Based on | Carries our patches? |
| --- | --- | --- | --- |
| `main` | Inherited from upstream (not used for our builds) | upstream `main` (3.8-SNAPSHOT) | No |
| `mirror/upstream-3.7` | Read-only mirror of the upstream `3.7` branch, for visibility and to pull future fixes from | upstream `3.7` | No |
| `weez-3.7` | **Production branch.** This is what the `kafka-connect` image builds. | tag `v3.7.0.Final` | **Yes** |

The production branch is pinned to the upstream **release tag** `v3.7.0.Final`, not to the moving
`3.7` branch head. The upstream `3.7` branch contains unreleased development commits that have
diverged from the `v3.7.0.Final` tag, so we deliberately do not ship it to production. We only move
`weez-3.7` forward when a new upstream **release tag** (e.g. `v3.7.1.Final`) is published.

## Our patches on top of `v3.7.0.Final`

- **Schema template canonicalization** for multi-tenant databases (gill): one DDL-identical schema
  per tenant is collapsed to a single built schema so the connector does not build hundreds of
  thousands of `TableSchema` objects at startup. See `TemplateSchemaMappingStorage` and the
  `schema.template.canonicalization.*` connector options.
- Removed the DCO sign-off requirement (CI check and docs).

## One-time setup for a working clone

```bash
git remote add upstream https://github.com/debezium/debezium.git
git fetch upstream
```

## Keeping the mirror up to date

```bash
git fetch upstream 3.7
git branch -f mirror/upstream-3.7 upstream/3.7
git push origin mirror/upstream-3.7
```

## Adopting a new upstream 3.7.x release into production

When upstream publishes a new release tag on the 3.7 series (for example `v3.7.1.Final`):

```bash
git fetch upstream --tags
git switch weez-3.7
git merge v3.7.1.Final        # merge the RELEASE TAG, not the moving 3.7 branch
# resolve any conflicts (our patches are small and isolated), then:
./mvnw clean install -pl debezium-connector-common,debezium-connector-postgres -am -Dquick
git push origin weez-3.7
```

We merge (rather than rebase) `weez-3.7` so the history is preserved and our patch commits stay
intact across upgrades.

## Building the artifacts used by the kafka-connect image

```bash
export JAVA_HOME=<path-to-jdk-21>
./mvnw clean install -pl debezium-connector-common,debezium-connector-postgres -am -Dquick
```

This produces the patched `debezium-connector-common` and `debezium-connector-postgres` jars that
replace the stock Debezium jars in the image.
