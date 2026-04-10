
## Databricks App
### Running Locally

**Requirements**

This should be possible with the following command but I've not got it working yet
```bash
databricks apps run-local
```

### Deployment

The bundle needs to recognise the existing state with (once only)
```bash
databricks bundle deployment bind goodreads_app goodreads
```

The resource can then be deployed with
```bash
databricks bundle validate
databricks bundle deploy
```

The app source code then needs to be deployed separately
```bash
databricks apps deploy goodreads-app \
  --source-code-path /Workspace/Shared/.bundle/goodreads_bundle/dev/files/app \
  --mode SNAPSHOT
```

The Unity catalog permissions need to be configured separately :(

It can be done in the CLI or via the UI

Find the service principal with
```bash
databricks service-principals list --output json | jq '.[] | select(.displayName | contains("goodreads")) | {displayName, applicationId}'
```

Then grant the permission
```bash
databricks grants update table goodreads.gold_pages_per_day \
  --json '{"changes": [{"principal": <service principal>, "add": ["SELECT"]}]}'

```

The env var will need to be provided from app.yml so the UI is probably simplest


The CI service principal also needs `CAN_MANAGE` on the app (one-time):

Currenlty not working, can be assigned in the UI
```bash
databricks apps set-permissions goodreads-app \
  --json '{"access_control_list": [{"service_principal_name": "<sp-id>", "permission_level": "CAN_MANAGE"}]}'
```

#### GitHub Actions

Two workflows are defined in `.github/workflows/`:

- **`deploy.yml`** — runs `databricks bundle deploy` on every push to `main`. Requires `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets set in the repo.
- **`setup.yml`** — manually triggered (`workflow_dispatch`). Runs the one-time `databricks bundle deployment bind` to link the bundle to an existing app. Takes the app name as an input (default: `goodreads-app`).

Several secrets/variables are needed in order for the pipeline to run including a service principle. This service principal will need to be given sufficient permissions to manage the UC table

#### Documentation:

- [Deploy a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy)
- [Manage Databricks apps with DABs](https://docs.databricks.com/aws/en/dev-tools/bundles/apps-tutorial)
- [Binding a resource to its remote counterpart](https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources#bind-a-resource-to-its-remote-counterpart)
- CLI reference:
  - [databricks bundle deployment](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#databricks-bundle-deployment)
  - [databricks bundle deploy](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#databricks-bundle-deploy)
  - [databricks apps deploy](https://docs.databricks.com/aws/en/dev-tools/cli/reference/apps-commands#databricks-apps-deploy)
- [databricks.yml schema](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#apps)


## Databricks Notebooks
TODO: document the pipeline