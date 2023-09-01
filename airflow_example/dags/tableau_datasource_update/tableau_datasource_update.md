## Overview
> The `tableau_datasource_update` DAG maintains our Tableau Datasources in Tableau Online.
> This DAG will update Columns, Folders, and Connections of a Datasource, based on the config YAML files.

## DAG Tasks
### `gather_datasource_update_tasks`
> - Downloads each Datasource from Tableau Online (without the extract)
> - Parses each Datasource and compares attributes of the Datasource against the YAML config files
> - Returns a dictionary of actions (tasks) to update for each Datasource

### `update_datasources`
> - Downloads each Datasource from Tableau Online (with the extract) that needs to be updated,
>  based on the tasks from the previous task
> - Updates the Datasources locally that were Downloaded, based on the tasks of actions from the previous task
> - Publishes the Datsources that were updated

- _Will skip datasources if they are specified in the `EXCLUDED_DATASOURCES` variable_
- _Will kick off `refresh_datasources` if there is a failure during this task_

### `refresh_datasources`
> - Refreshes all Datasources in Tableau Online

- _Will skip datasources if they are specified in the `NO_REFRESH_DATASOURCES` variable_