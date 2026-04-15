# Deployment

The project is deployed via [Databricks Asset Bundles (DAB)](https://docs.databricks.com/aws/en/dev-tools/bundles/). The bundle definition is in `databricks.yml`.

## GitHub Actions (normal workflow)

Three workflows are defined in `.github/workflows/`:

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `test.yml` | push / PR | Runs pytest on `notebooks/` and `app/` (Python 3.14) |
| `deploy.yml` | push to `main` | Deploys bundle + app to dev, patches UC table resources |
| `setup.yml` | manual (`workflow_dispatch`) | One-time bind of bundle to existing app |

### Required GitHub secrets / variables

| Name | Type | Description |
|------|------|-------------|
| `DATABRICKS_HOST` | variable | Workspace URL |
| `DATABRICKS_CLIENT_ID` | secret | Service principal OAuth client ID |
| `DATABRICKS_CLIENT_SECRET` | secret | Service principal OAuth client secret |

The deploy workflow authenticates as a service principal. That principal needs sufficient permissions — see [permissions.md](./permissions.md).

### Patching Resources (workaround)

`bundle deploy` resets app resources to only what is declared in `databricks.yml`. Because the DAB YAML schema does not support `securable_type: TABLE`, this step fetches the current resource list and upserts the UC table entries via the REST API.

> `databricks.yml` is the single source of truth for all bundle-manageable resources. The patch step only needs updating when the UC table extras list changes.

---

## Reference

- [Deploy a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy)
- [Manage Databricks apps with DABs](https://docs.databricks.com/aws/en/dev-tools/bundles/apps-tutorial)
- [Binding a resource to its remote counterpart](https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources#bind-a-resource-to-its-remote-counterpart)
- [databricks bundle deployment CLI](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#databricks-bundle-deployment)
- [databricks bundle deploy CLI](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#databricks-bundle-deploy)
- [databricks apps deploy CLI](https://docs.databricks.com/aws/en/dev-tools/cli/reference/apps-commands#databricks-apps-deploy)
- [databricks.yml schema — apps](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#apps)
