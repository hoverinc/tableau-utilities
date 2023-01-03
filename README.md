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

### CLI Usage

#### Help
1. See the top level CLI arguments including the commands.
```commandline
tableau_utilities --help
```

2. See the help for a `command`.  Each command has arguments for a different grouping of actions.
```commandline
tableau_utilities server_operate --help
```

#### Authentication Options
1. Pass your credentials into the command
```commandline
 tableau_utilities --auth args_token --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az server_info --list_object datasource --list_format names
```

2. Use a secrets yaml file
```commandline
 tableau_utilities --auth settings_yaml  --settings_path my_settings.yaml --list_object datasource --list_format names
```

3. Use enviroment variables. This example also uses the 1password cli for retrieving the variables and putting them in the environment
```commandline
tableau_utilities --auth args_token --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az  server_info --list_object datasource --list_format names
```
#### Examples for each command area

* `server_info`: Lists all datasources in your site ordered by ID
```commandline
tableau_utilities --auth args_token --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az server_info --list_object datasource --list_format ids_names --list_sort_field id
```

* `server_operate`: Download a datasource by name
```commandline
tableau_utilities --auth args_token --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az server_info server_operate --action_type download --object_type datasource --name 'My Datasource Name'  --project_name 'My Project Name'
```

* `generate_config`: Generate a config from a datasource in online/server
```commandline
tableau_utilities --auth args_token --token_name my_token_name --token_secret 1q2w3e4r5t6y7u8i9o --site mysitename --server 10az server_info --datasource_source online --datasource_name 'My Awseome Datasource' --datasource_project_name 'My Team Project'

```

* `generate_config`: Generate a config from a local file. Add a file prefix and print the debugging logs to the console
```commandline
tableau_utilities --debugging_logs generate_config --datasource_source local --datasource_path '/Users/jayrosenthal/code/tableau-utilities/tmp_tdsx_and_config/My Awesome Datasource.tdsx' --file_prefix
```

# `merge_config`: Merge a new config into your main config
```commandline
tableau_utilities merge_config --existing_config /Users/jayrosenthal/code/tableau-utilities/tmp_tdsx_and_config/main__column_config.json --additional_config /Users/jayrosenthal/code/tableau-utilities/tmp_tdsx_and_config/new__column_config.json
```


TO DO: ADD THIS FUNCTIONALITY TO THE NEW CLI
- Add column to datasource
  - `tableau_utilities --tdsx path/to/file.tdsx --add_column --name "column_name" --folder "Folder Name" --caption "Column Alias" --desc "column description"`

### Development
- `pip install -r requirements/dev.txt`
- Create `settings.yaml` file in the `tableau_utilities` directory
  - See `sample_settings.yaml` for an example
- `test_tableau_utilities`
  - Add tests as needed
  - Run when making changes

## Maintenance

This project is actively maintained by the Data Platform team at [@hoverinc][hover-github-link].

[hover-github-link]: https://github.com/hoverinc
