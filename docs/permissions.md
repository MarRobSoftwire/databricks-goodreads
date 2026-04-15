# Permissions

One-time grants needed before the pipeline and app will work. Can be done via CLI or the Databricks UI.

## Find the service principal

```bash
databricks service-principals list --output json \
  | jq '.[] | select(.displayName | contains("goodreads")) | {displayName, applicationId}'
```

## UC table grants

```bash
databricks grants update table goodreads.gold_pages_per_day \
  --json '{"changes": [{"principal": "<sp-id>", "add": ["SELECT"]}]}'

databricks grants update table goodreads.gold_genre \
  --json '{"changes": [{"principal": "<sp-id>", "add": ["SELECT"]}]}'
```

## CI service principal: CAN_MANAGE on the app

```bash
databricks apps set-permissions goodreads-app \
  --json '{"access_control_list": [{"service_principal_name": "<sp-id>", "permission_level": "CAN_MANAGE"}]}'
```

> If the CLI command fails, assign via the UI instead.
