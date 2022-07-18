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

- Lists all datasources in your site
  - `tableau_utilities --user <login> --password <password> --site <site name> --server <server address> --list_datasources`
  - OR `tableau_utilities --settings_path ./settings.yaml --list_datasources`
- Download a datasource
  - `tableau_utilities --user <login> --password <password> --site <site name> --server <server address> --download_ds --name "Datasource Name" --project "Project Name"`
  - OR `tableau_utilities --settings_path ./settings.yaml --download_ds --name "Datasource Name" --project "Project Name"`
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
