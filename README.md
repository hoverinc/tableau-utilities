# tableau-utilities

A module and CLI Utility for managing Tableau objects, locally, and in Tableau Online.

## Quick start

### Installation

#### From pypi

- `pip install tableau-utilities`

#### Locally using pip

- `cd tableau-utilities`
- `pip install ./`

### Confirm installation

- `which tableau_utilities`
  - _Describes where tableau-utilities has been installed_
- `tableau_utilities --help`
  - _Should populate a list of available commands_

### Module Usage

#### Sample

```python
import tableau_utilities as tu
from my_secrets import tableau_creds


def main():
    # The datasource can be defined either by the ID, or name and project
    datasource_id = 'abc123'
    datasource_name = None
    project_name = None

    # Create a Tableau Connection
    ts = tu.TableauServer(**tableau_creds)
    # Download a datasource
    tdsx_path = ts.download_datasource(dsid=datasource_id, name=datasource_name, project=project_name)
    # Extract the TDS file from the TDSX for making updates
    tds_path = tu.extract_tds(tdsx_path)
    tds = tu.TDS(tds_path)
    # Add a column to the datasource
    tds.add(
        item_type='column',
        column_name='Calculation_1',
        remote_name='Calculation_1',
        caption='Max Created Datetime',
        folder_name='Time Dimensions',
        role='dimension',
        role_type='ordinal',
        datatype='datetime',
        desc='The maximum Created Datetime.',
        calculation='MAX([Created Datetime])'
    )
    # Update the datasource from alterations made to the TDS
    tu.update_tdsx(tdsx_path, tds_path)
    # Publish the datasource
    ts.publish_datasource(tdsx_path, dsid=datasource_id, name=datasource_name, project=project_name)


if __name__ == '__main__':
    main()

```

### CLI Usage

- `tableau_utilities --user <login> --password <password> --site <site name> --server <server address> --list_datasources`
  - Lists all datasources in your site
- `tableau_utilities --user <login> --password <password> --site <site name> --server <server address> --download_ds --name "Datasource Name" --project "Project Name"`
  - Download a datasource
- `tableau_utilities --tdsx path/to/file.tdsx --add_column --name "column_name" --folder "Folder Name" --caption "Column Alias" --desc "column description"`
  - Add column to datasource

## Maintenance

This project is actively maintained by the Data Platform team at [@hoverinc][hover-github-link].

[hover-github-link]: https://github.com/hoverinc
