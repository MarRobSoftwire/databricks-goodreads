# Deployment

Managed via Databricks Asset Bundles (`databricks.yml`).

## GitHub Actions

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `test.yml` | push / PR | pytest on `notebooks/` and `app/` |
| `deploy.yml` | push to `main` | bundle deploy + app deploy + UC table patch |
| `setup.yml` | manual | one-time bundle bind to existing app |

**Required secrets/variables:** `DATABRICKS_HOST` (variable), `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` (secrets).

### Patching Resources (workaround)

`bundle deploy` resets app resources to only what is declared in `databricks.yml`. Because the DAB YAML schema does not support `securable_type: TABLE`, this step fetches the current resource list and upserts the UC table entries via the REST API. The list of extra resources lives in **`deploy/databricks-app-uc-resources.json`** (resource `name` values must match `valueFrom` in `app/app.yml`).

> `databricks.yml` is the single source of truth for all bundle-manageable resources. The patch step only needs updating when that JSON list changes.

---

## Reference

- [Deploy a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy)
- [Manage Databricks apps with DABs](https://docs.databricks.com/aws/en/dev-tools/bundles/apps-tutorial)
- [Binding a resource to its remote counterpart](https://docs.databricks.com/aws/en/dev-tools/bundles/migrate-resources#bind-a-resource-to-its-remote-counterpart)
- [databricks bundle deployment CLI](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#databricks-bundle-deployment)
- [databricks bundle deploy CLI](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#databricks-bundle-deploy)
- [databricks apps deploy CLI](https://docs.databricks.com/aws/en/dev-tools/cli/reference/apps-commands#databricks-apps-deploy)
- [databricks.yml schema — apps](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#apps)
