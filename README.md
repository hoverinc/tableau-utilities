# tableau-utilities

A module and CLI Utility for managing Tableau objects, locally, and in Tableau Online.

## Quick start

### Installation

#### From pypi
- `pip install tableau-utilities`

#### Locally using pip
- `cd tableau-utilities`
- `pip install ./`

#### Confirm installation
- `which tableau_utilities`
  - _Describes where tableau-utilities has been installed_
- `tableau_utilities --help`
  - _Should populate a list of available commands_

### Module Usage

#### Sample

```python
from tableau_utilities import Datasource, TableauServer
from tableau_utilities import tableau_file_objects as tfo
from my_secrets import tableau_creds


def main():
    # The datasource identified by the ID
    datasource_id = 'abc123'

    # Create a Tableau Connection
    ts = TableauServer(**tableau_creds)
    # Download a Datasource
    datasource_path = ts.download_datasource(datasource_id=datasource_id)
    # Define a Datasource object from the datasource_path
    datasource = Datasource(datasource_path)
    # Define a new folder
    folder = tfo.Folder(name='Time Dimensions')
    # Define a new Column
    column = tfo.Column(
        name='Calculation_1',
        caption='Max Created Datetime',
        role='dimension',
        type='ordinal',
        datatype='datetime',
        desc='The maximum Created Datetime.',
        calculation='MAX([Created Datetime])'
    )
    # Add the new column to the new folder, as a folder-item
    folder_item = tfo.FolderItem(name=column.name)
    folder.folder_item.add(folder_item)
    # Add the column and folder to the datasource
    datasource.columns.add(column)
    datasource.folders_common.add(folder)
    # Enforce the Column, to update the Metadata
    datasource.enforce_column(column, remote_name='max_created_at')
    # Save changes to the Datasource
    datasource.save()
    # Publish & Overwrite the Datasource
    ts.publish_datasource(datasource_path, datasource_id=datasource_id)


if __name__ == '__main__':
    main()

```

## CLI Usage

### Help
See the top level CLI arguments including the commands.
```commandline
tableau_utilities --help
```

See the help for a `command`.
Each command has arguments for a different grouping of actions.
```commandline
tableau_utilities server_operate --help
```

### Authentication Options
Pass your credentials into the command
```commandline
tableau_utilities --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az server_info --list_object datasource --list_format names
```

Use a settings YAML file
```commandline
tableau_utilities  --settings_path my_settings.yaml server_info --list_object datasource --list_format names
```

Use environment variables
```commandline
tableau_utilities server_info --list_object datasource --list_format names
```

Using the 1password CLI with op run
```commandline
op run --env-file=.env -- tableau_utilities server_info list_--list_object datasource --list_format names
```

### Examples for each command

#### server_info
Lists all datasources in your site ordered by ID
```commandline
tableau_utilities --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az server_info --list_object datasource --list_format ids_names --list_sort_field id
```

#### server_operate
Download a datasource by name
```commandline
tableau_utilities --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site_name mysitename --server 10az --name 'My Awesome Datasource' --project_name 'My Fabulous Project' server_operate --download datasource
```

Publish Datasource with embedded connection credentials
```commandline
tableau_utilities -tn my_token_name -ts 1q2w3e4r5t6y7u8i9o -sn mysitename -s 10az -n 'My Awesome Datasource' -pn 'My Fabulous Project' --file_path '/Downloads/My Awesome Datasource.tdsx' --conn_user username --conn_pw abc123 server_operate --publish datasource
```

Embed Connection credentials for a Datasource
```commandline
tableau_utilities -tn my_token_name -ts 1q2w3e4r5t6y7u8i9o -sn mysitename -s 10az -n 'My Awesome Datasource' -pn 'My Fabulous Project' --conn_user username --conn_pw abc123 server_operate --embed_connection
```

#### datasource
Save the TDS for a datasource from a local datasource to view the raw XML
```commandline
tableau_utilities --location local --file_path '/Downloads/My Awesome Datasource.tdsx' --save_tds  datasource
```

Save the TDS for a datasource from an online datasource to view the raw XML
```commandline
tableau_utilities --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az --location online --name 'My Awesome Datasource' --project_name 'My Cool Project' --save_tds datasource
```

Change the folder for a column
```commandline
tableau_utilities --location local --file_path '/Downloads/Metadata Alter.tdsx' datasource --column_name COLUMN_NAME --folder_name 'Folder Name'
```

Update/Add attributes for a column
```commandline
tableau_utilities --location local --file_path '/Downloads/Metadata Alter.tdsx' datasource --column_name COLUMN_NAME --remote_name COLUMN_NAME_FROM_CONNECTION --caption 'My Pretty Column Name' --persona string_dimension --desc 'A help description for Tableau users to understand the data' 
```

Delete folder
```commandline
tableau_utilities --location local --file_path '/Downloads/Metadata Alter.tdsx' datasource --folder_name 'Folder Name' --delete folder
```

Enforce Datasource connection credentials
```commandline
tableau_utilities -l local -f '/Downloads/Metadata Alter.tdsx' --conn_type snowflake --conn_host https://some.url.com --conn_user username --conn_pw password --conn_db database_name --conn_schema schema_name --conn_role role_name --conn_warehouse warehouse_name datasource --embed_connection
```

#### generate_config

Generate a config from a datasource in online/server
```commandline
tableau_utilities --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az --location online --name 'My Awseome Datasource' --project_name 'My Team Project' generate_config
```

Generate a config from a datasource in online/server and add definitions from a csv
```commandline
tableau_utilities --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az --location online --name 'My Awseome Datasource' --project_name 'My Team Project' generate_config --definitions_csv /Desktop/new_descriptions.csv
```

Generate a config from a local file. Add a file prefix and print the debugging logs to the console
```commandline
tableau_utilities --debugging_logs generate_config --location local --file_path '/code/tableau-utilities/tmp_tdsx_and_config/My Awesome Datasource.tdsx' --file_prefix
```

#### csv_config
Write the config to a csv with 1 row per field per datasource
```commandline
 tableau_utilities csv_config --config_list /code/airflow/dags/tableau/configs/column_config.json /code/airflow/dags/tableau/configs/tableau_calc_config.json
```

#### merge_config
Merge a new config into your main config
```commandline
tableau_utilities merge_config --merge_with config --existing_config /code/tableau-utilities/tmp_tdsx_and_config/main__column_config.json --additional_config /code/tableau-utilities/tmp_tdsx_and_config/new__column_config.json --merged_config code/dbt-repo/tableau_datasource_configs/column_config
```

Merge data defintions from a csv into your main config
```commandline
tableau_utilities --definitions_csv /Desktop/new_definitions.csv merge_config --merge_with csv  merge_config --existing_config code/dbt-repo/tableau_datasource_configs/column_config.json --merged_config code/dbt-repo/tableau_datasource_configs/column_config
```


### Development
- `pip install -r requirements.txt`
- Create `settings.yaml` file in the directory where `tableau_utilities` is called.
  - See `sample_settings.yaml` for an example.
- `test_tableau_utilities`
  - Add tests as needed
  - Run when making changes

## Maintenance

This project is actively maintained by the Data Platform team at [@hoverinc][hover-github-link].

[hover-github-link]: https://github.com/hoverinc
