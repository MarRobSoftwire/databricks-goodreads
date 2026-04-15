## Databricks App

Avoid creating large files - split them up where possible

### Environment Variables
Environment variables for Databricks resources should be injected via the `app.yml` file.
These environment variables should be added as `resources` in the `databricks.yml` file.
If the resource is not supported by the [DAB syntax](https://docs.databricks.com/aws/en/dev-tools/bundles/resources#apps) (e.g. UC tables) then it will need to be injected in the [deploy action](./.github/workflows/deploy.yml) — add the entry to `deploy/databricks-app-uc-resources.json`.

## Databricks Notebooks

Avoid containing the entirety of the notebook in a single file — use utils where possible.
Core logic belongs in `*_utils.py`; notebooks are thin orchestration wrappers. This keeps unit tests straightforward.
