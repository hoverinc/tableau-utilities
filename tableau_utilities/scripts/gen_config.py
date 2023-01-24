import json
from pathlib import Path

import pandas as pd

from tableau_utilities.general.config_column_persona import personas
from tableau_utilities.general.funcs import convert_to_snake_case
from tableau_utilities.scripts.server_operate import get_object_list, fill_in_id_name_project
from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def load_csv_with_definitions(file=None):
    """ Returns a dictionary with the definitions from a csv. The columns are expected to include column_name and description

    Args:
        file: The path to the .csv file with the definitions. The csv must include a column_name and description.

    Returns:
        dictionary mapping column name to definition

    """

    df = pd.read_csv(file)
    df.columns = df.columns.str.lower()
    definitions = df.to_dict('records')

    # Check that the csv contains column_name and description headers
    column_names = list(df.columns)
    if 'column_name' not in column_names or 'description' not in column_names:
        raise ValueError('The .csv must contain a column_name and a description column.')

    # print(definitions)
    definitions_mapping = {}

    for column in definitions:
        if str(column['description']) != 'nan':
            definitions_mapping[column['column_name']] = column['description']

    return definitions_mapping


def download_datasource(server, datasource_id):
    """ Downloads the specified datasources

    Args:
        server (TableauServer): A Tableau server object
        datasource_name: The name of the datasource to download

    Returns:
        datasource_path: The path of the datasource that was downloaded
    """

    datasource_path = server.download_datasource(datasource_id, include_extract=False)
    return datasource_path


def choose_persona(role, role_type, datatype):
    """  The config relies on a persona which is a combination of role, role_type and datatype for each column.
    This returns the persona name or raises an exception if the combination is not found

    Args:
        role: dimension or measure
        role_type: nominal, ordinal, or quantitative
        datatype: string, date, datetype, real, or boolean

    """

    persona_name = None
    for persona in personas:
        for k, v in persona.items():
            if role == v['role'] and role_type == v['role_type'] == role_type and datatype == v['datatype']:
                persona_name = k
                break

    if persona_name is not None:
        return persona_name
    else:
        raise ValueError(
            f"There is no persona for the combination of ROLE {role}, ROLE_TYPE {role_type}, and DATATYPE {datatype}'")


def get_metadata_record_columns(datasource_name, datasource_path, debugging_logs=False):
    """ Builds a column config for columns that are only in metadata records and not in column objects

    Args:
        datasource_name: The name of the datasource
        datasource_path: The path to the of the datasource
        debugging_logs: Prints information to consolde if true

    """

    rows = dict()
    metadata_records = [c.dict() for c in Datasource(datasource_path).connection.metadata_records]
    rows.setdefault(datasource_name, [])
    rows[datasource_name].extend(metadata_records)

    metadata_record_columns = {}

    for m in metadata_records:
        if m['@class'] == 'column':

            # I think it's low risk to assume these data types are all dimensions
            if m["local-type"] == 'string':
                persona = 'string_dimension'
            elif m["local-type"] == 'date':
                persona = 'date_dimension'
            elif m["local-type"] == 'datetime':
                persona = 'datetime_dimension'
            elif m["local-type"] == 'boolean':
                persona = 'boolean_dimension'
            # There's no good way to assume if integer & real data types are dimensions or measures
            # Instead of making assumptions on these the config will skip adding these fields and users will
            # need to make manual adjustments
            # Leaving these ELIF's in case there is a better way to do this later
            elif m["local-type"] == 'integer':
                persona = None
            elif m["local-type"] == 'real':
                persona = None

            metadata_record_columns[m['remote-name']] = {'persona': persona,
                                                         "datasources": [
                                                             {
                                                                 "name": datasource_name,
                                                                 "local-name": m['local-name'][1:-1],
                                                                 "sql_alias": m['remote-name']
                                                             },
                                                         ]

                                                         }

            if debugging_logs:
                print(m)
                print(persona)
                print(m['remote-name'])
                print(metadata_record_columns[m['remote-name']])

    return metadata_record_columns


def create_column_config(columns, datasource_name, folder_mapping, metadata_record_columns, definitions_mapping,
                         debugging_logs):
    """ Generates a list of column configs with None for a folder

    Args:
        columns: The column dictionary from the datasource
        datasource_name: The name of the datasource
        folder_mapping: A list of dictionaries mapping column name to folder name
        metadata_record_columns: The configs from the metadata records to use if there is no column object
        definitions_mapping: The mapping of definitions from a csv

    ```{
      "Salesforce Opportunity Id": {
        "description": "The 18 digit account Id for a Salesforce opportunity",
        "folder": Name,
        "persona": "string_dimension",
        "datasources": [
          {
            "name": "Opportunity",
            "local-name": "SALESFORCE_OPPORTUNITY_ID",
            "sql_alias": "SALESFORCE_OPPORTUNITY_ID"
          }
        ]
      }
      ````

    Returns:
        column_configs: A dictionary with the configs for fields that are pulled from the source database
        calculated_column_configs: A dictionary with the configs for calculated fields added in the workbook that built
            the datasource

    """

    column_configs = {}
    calculated_column_configs = {}
    column_name_list = []

    for c in columns:

        column_name = c['@name'][1:-1]
        # Skip internal object columns
        if column_name.startswith('__tableau_internal_object_id__'):
            continue

        # Keeps a list of column names from the column object.
        column_name_list.append(column_name)

        # Make a title case caption from the database name if there is no caption
        if '@caption' in c:
            caption = c['@caption']
        else:
            caption = column_name.replace('_', ' ').title()

        # Skip the table datatype for now
        if c['@datatype'] == 'table':
            pass
        else:
            persona = choose_persona(role=c['@role'], role_type=c['@type'], datatype=c['@datatype'])

        # Takes the description from the csv if there is one
        # Assumes the csv  is the source of truth if there are definitions in both
        if definitions_mapping is not None:
            if caption in definitions_mapping and definitions_mapping[caption] is not None and \
                    (isinstance(definitions_mapping[caption], str) and len(definitions_mapping[caption]) > 0):
                description = definitions_mapping[caption]
            elif 'desc' in c:
                description = c['desc']['formatted-text']['run']
            else:
                description = ''
        elif 'desc' in c:
            description = c['desc']['formatted-text']['run']
        else:
            description = ''

        folder_name = None
        if column_name in folder_mapping.keys():
            folder_name = folder_mapping[column_name]

        # Calculations are written to a separate config in the Airflow DAG
        if 'calculation' in c:

            if debugging_logs:
                print('-' * 30)
                print('CALCULATED COLUMN CONFIG FIELD')
                print(caption)
                print(c)
                print(calculated_column_configs[caption])

            calculated_column_configs[caption] = {
                "description": description,
                "calculation": c['calculation']['@formula'],
                "folder": folder_name,
                "persona": persona,
                "datasources": [
                    {
                        "name": datasource_name,
                        "local-name": column_name,
                        "sql_alias": column_name
                    },
                ]
            }

            # Optional Properties to Add
            if '@fiscal_year_start' in c:
                calculated_column_configs[caption]['fiscal_year_start'] = c['@fiscal_year_start']
            if '@default-format' in c:
                calculated_column_configs[caption]['default_format'] = c['@default-format']

        else:

            if debugging_logs:
                print('-' * 30)
                print('COLUMN CONFIG FIELD')
                print(caption)
                print(c)
                print(column_configs[caption])

            column_configs[caption] = {
                "description": description,
                "folder": folder_name,
                "persona": persona,
                "datasources": [
                    {
                        "name": datasource_name,
                        "local-name": column_name,
                        "sql_alias": column_name
                    },
                ]
            }

            # Optional Properties to Add
            if '@fiscal_year_start' in c:
                column_configs[caption]['fiscal_year_start'] = c['@fiscal_year_start']
            if '@default-format' in c:
                column_configs[caption]['default_format'] = c['@default-format']



    # Add column configs for metadata_record columns when there wasn't a column object already
    # This is only need for non-calulated fields
    for k, v in metadata_record_columns.items():
        if k in column_name_list:
            pass
        else:
            caption = k.replace('_', ' ').title()

            if v['persona'] in ['string_dimension', 'date_dimension', 'datetime_dimension', 'boolean_dimension']:
                column_configs[caption] = {
                    "description": '',
                    "folder": None,
                    "persona": v['persona'],
                    "datasources": v['datasources']
                }

            if debugging_logs:
                print('-' * 30)
                print('METADATA RECORD COLUMN ADDED')
                print(caption)
                print(column_configs[caption])

    return column_configs, calculated_column_configs


def build_folder_mapping(datasource_path):
    """  Builds a dictionary mapping columns to folders

    Args:
      The path to the datasource

    Returns:
        A dictionary with the mapping like

        ```
        {'column1': 'folderA',
         'column2': 'folderA',
         'column3': 'folderB',
        ```

    """

    folders = [c.dict() for c in Datasource(datasource_path).folders_common]

    mappings = {}
    for f in folders:
        folder_name = f['@name']

        if 'folder-item' in f:
            for item in f['folder-item']:
                field_name = item['@name'][1:-1]
                mappings[field_name] = folder_name

    return mappings


def extract_columns(datasource_name, datasource_path):
    """ Extract the columns into a dictionary from the datasource

    Args:
        datasource_name: The name of the datasource
        datasource_path: The path to the tdsx file

    Returns:
        A list of dictionaries with the column information

    """

    rows = dict()
    columns = [c.dict() for c in Datasource(datasource_path).columns]
    rows.setdefault(datasource_name, [])
    rows[datasource_name].extend(columns)

    return columns


def generate_config(args, server=None):
    """ Downloads a datasource and saves configs for that datasource

    """

    # Set variables from the args
    definitions_csv_path = args.definitions_csv
    debugging_logs = args.debugging_logs
    location = args.location
    id = args.id
    datasource_name = args.name
    project_name = args.project_name
    datasource_path = args.file_path

    # Set file_prefix to false when this function is called from the merge_config namespace
    try:
        file_prefix = args.file_prefix
    except:
        file_prefix = False

    # Get the datasource name from the path
    if location == 'local':
        datasource_name = Path(datasource_path).stem
        print(f'BUILDING CONFIG FOR: {datasource_name} {datasource_path} ')

    # Download the datasouce and set values for
    elif location == 'online':
        object_list = get_object_list(object_type='datasource', server=server)
        id, datasource_name, project_name = fill_in_id_name_project(id, datasource_name, project_name, object_list)
        datasource_path = download_datasource(server, id)
        print(
            f'GETTING DATASOURCE ID: {id}, NAME: {datasource_name}, PROJECT NAME: {project_name}, INCLUDE EXTRACT false')

    # Get column information from the metadata records
    metadata_record_columns = get_metadata_record_columns(datasource_name, datasource_path, debugging_logs)

    # Get the mapping of definitions from the csv
    definitions_mapping = None
    if definitions_csv_path is not None:
        definitions_mapping = load_csv_with_definitions(file=definitions_csv_path)

    # Extract the columns and folders. Build the new config
    columns = extract_columns(datasource_name, datasource_path)
    folder_mapping = build_folder_mapping(datasource_path)
    column_configs, calculated_column_configs = create_column_config(columns=columns,
                                                                     datasource_name=datasource_name,
                                                                     folder_mapping=folder_mapping,
                                                                     metadata_record_columns=metadata_record_columns,
                                                                     definitions_mapping=definitions_mapping,
                                                                     debugging_logs=debugging_logs)

    # Sort configs
    column_configs = dict(sorted(column_configs.items()))
    calculated_column_configs = dict(sorted(calculated_column_configs.items()))

    datasource_name_snake = convert_to_snake_case(datasource_name)
    output_file_column_config = 'column_config.json'
    output_file_calculated_column_config = 'tableau_calc_config.json'

    if file_prefix:
        output_file_column_config = f'{datasource_name_snake}__{output_file_column_config}'
        output_file_calculated_column_config = f'{datasource_name_snake}__{output_file_calculated_column_config}'

    with open(output_file_column_config, "w") as outfile:
        json.dump(column_configs, outfile)

    with open(output_file_calculated_column_config, "w") as outfile:
        json.dump(calculated_column_configs, outfile)

    print('DATSOURCE PATH:', datasource_path)
    print('COLUMN CONFIG PATH:', output_file_column_config)
    print('CALCULATED COLUMN CONFIG PATH:', output_file_calculated_column_config)

    return output_file_column_config, output_file_calculated_column_config
