# Permissions

Several one-time permission grants are needed before the pipeline and app function correctly. These are separate from the bundle deployment and must be done manually (CLI or Databricks UI).

## Service principal

The pipeline notebooks run as a service principal. Find it with:

```bash
databricks service-principals list --output json \
  | jq '.[] | select(.displayName | contains("goodreads")) | {displayName, applicationId}'
```

## Unity Catalog table access

The app service principal needs `SELECT` on the Gold tables it queries:

```bash
databricks grants update table goodreads.gold_pages_per_day \
  --json '{"changes": [{"principal": "<service-principal-id>", "add": ["SELECT"]}]}'

databricks grants update table goodreads.gold_genre \
  --json '{"changes": [{"principal": "<service-principal-id>", "add": ["SELECT"]}]}'
```

> The app resources for these tables cannot be expressed in the `databricks.yml` schema (only `VOLUME` is supported for `uc_securable`, not `TABLE`). They are injected on each deploy by the "Patch UC table resource" step in `deploy.yml`.

## App management for CI service principal

The CI service principal needs `CAN_MANAGE` on the app to deploy it. Assign via the UI, or with:

```bash
databricks apps set-permissions goodreads-app \
  --json '{"access_control_list": [{"service_principal_name": "<sp-id>", "permission_level": "CAN_MANAGE"}]}'
```

> This command may not work via CLI in all workspace configurations — the UI is the reliable fallback.
