import json
from pathlib import Path

import pandas as pd

from tableau_utilities.general.cli_styling import Color, Symbol
from tableau_utilities.general.config_column_persona import get_persona_by_attribs, get_persona_by_metadata_local_type
from tableau_utilities.general.funcs import convert_to_snake_case
from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def load_csv_with_definitions(file=None):
    """ Returns a dictionary with the definitions from a csv. The columns are expected to include column_name and description

    Args:
        file: The path to the .csv file with the definitions. The csv must include a column_name and description.

    Returns:
        dictionary mapping column name to definition

    """

    definitions_mapping = dict()
    df = pd.read_csv(file)
    df.columns = df.columns.str.lower()
    definitions = df.to_dict('records')

    # Check that the csv contains column_name and description headers
    column_names = list(df.columns)
    if 'column_name' not in column_names or 'description' not in column_names:
        raise ValueError('The .csv must contain a column_name and a description column.')

    for column in definitions:
        if str(column['description']) != 'nan':
            definitions_mapping[column['column_name']] = column['description']

    return definitions_mapping


def choose_persona(role, role_type, datatype, caption):
    """  The config relies on a persona which is a combination of role, role_type and datatype for each column.
    This returns the persona name or raises an exception if the combination is not found

    Args:
        role: dimension or measure
        role_type: nominal, ordinal, or quantitative
        datatype: string, date, datetype, real, or boolean
        caption: The name of the column for the persona. Used in error message

    """
    p = get_persona_by_attribs(role, role_type, datatype)
    if p:
        return p

    raise ValueError(
        f"There is no persona for the combination of ROLE {role}, ROLE_TYPE {role_type}, and DATATYPE {datatype}."
        f"Error on column {caption}'"
    )


def get_metadata_record_config(metadata_records, datasource_name, debugging_logs=False):
    """ Builds a column config for columns that are only in metadata records and not in column objects

    Args:
        metadata_records (Datasource.connection.metadata_records): The metadata_records list from the Datasource object
        datasource_name (str): The name of the datasource
        debugging_logs: Prints information to consolde if true
    """
    metadata_record_columns = dict()

    for m in metadata_records:
        if m.class_name == 'column':
            local_name = m.local_name[1:-1]
            persona = get_persona_by_metadata_local_type(m.local_type) or ''

            metadata_record_columns[local_name] = {
                'persona': persona,
                "datasources": [
                    {
                        "name": datasource_name,
                        "local-name": local_name,
                        "sql_alias": m.remote_name
                    },
                ]
            }

            if debugging_logs:
                print(m)
                print(persona)
                print(local_name, ':', m.remote_name)
                print(metadata_record_columns[local_name])

    return metadata_record_columns


def create_column_config(columns, datasource_name, folder_mapping, metadata_record_columns, definitions_mapping,
                         debugging_logs):
    """ Generates a list of column configs with None for a folder

    Args:
        columns (Datasource.columns): The list of Column objects from the Datasource object
        datasource_name (str): The name of the datasource
        folder_mapping (dict): A dict mapping column name to folder name
        metadata_record_columns (dict): The config based on metadata records; used if there is no column object
        definitions_mapping (dict): The mapping of definitions from a csv
        debugging_logs (bool): True to print debugging logs

    Returns:
        column_config (dict): A dict config for fields that are pulled from the source database
        calculated_column_configs (dict): A dictionary with the configs for calculated fields added in the workbook that built
            the datasource

        Sample column_config Output: {
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
    """

    column_config = dict()
    calculated_column_configs = dict()
    column_name_list = list()

    for column in columns:
        column_name = column.name[1:-1]

        # Skip internal object columns
        if column_name.startswith('__tableau_internal_object_id__'):
            if debugging_logs:
                print(f'Skipping {column_name}: starts with __tableau_internal_object_id__ ')
            continue
        # Skip columns with the 'table' datatype for now
        if column.datatype == 'table':
            if debugging_logs:
                print(f'Skipping {column_name}: column.datatype == "table" ')
            continue

        # Keeps a list of column names from the column object.
        column_name_list.append(column_name)

        # Make a title case caption from the database name if there is no caption
        if column.caption:
            caption = column.caption
        else:
            caption = column_name.replace('_', ' ').title()

        persona = choose_persona(role=column.role, role_type=column.type, datatype=column.datatype,
                                 caption=caption)

        # Takes the description from the csv if there is one
        # Assumes the csv  is the source of truth if there are definitions in both
        if definitions_mapping.get(caption, '') != '':
            description = definitions_mapping[caption]
        else:
            description = column.desc or ''

        folder_name = folder_mapping.get(column_name)

        column_dict = {
            "description": description,
            "folder": folder_name,
            "persona": persona,
            "datasources": [
                {
                    "name": datasource_name,
                    "local-name": column_name
                },
            ]
        }
        # Optional Properties to Add
        if column.fiscal_year_start:
            column_dict['fiscal_year_start'] = column.fiscal_year_start
        if column.default_format:
            column_dict['default_format'] = column.default_format

        if debugging_logs:
            print('-' * 30)
            print(caption)
            print(column.dict())
        # Calculations are written to a separate config in the Airflow DAG
        if column.calculation:
            calculated_column_configs[caption] = column_dict
            calculated_column_configs[caption]["calculation"] = column.calculation
            if debugging_logs:
                print('CALCULATED COLUMN CONFIG FIELD')
                print(calculated_column_configs[caption])
        else:
            if column_name in metadata_record_columns:
                sql_alias = metadata_record_columns[column_name]['datasources'][0]['sql_alias']
            else:
                sql_alias = column_name
            column_config[caption] = column_dict
            column_config[caption]['datasources'][0]['sql_alias'] = sql_alias
            if debugging_logs:
                print('COLUMN CONFIG FIELD')
                print(column_config[caption])

    # Add column configs for metadata_record columns when there wasn't a column object already
    # This is only need for non-calulated fields
    for k, v in metadata_record_columns.items():
        if k in column_name_list:
            continue

        caption = v['datasources'][0]['sql_alias'].replace('_', ' ').title()

        if v['persona']:
            column_config[caption] = {
                "description": '',
                "folder": None,
                "persona": v['persona'],
                "datasources": v['datasources']
            }

            if debugging_logs:
                print('-' * 30)
                print('METADATA RECORD COLUMN ADDED')
                print(caption)
                print(v['persona'])
                print(column_config[caption])
        else:
            if debugging_logs:
                print('-' * 30)
                print('METADATA RECORD COLUMN SKIPPED - Missing persona')
                print(caption)
                print(k, ':', v)

    return column_config, calculated_column_configs


def build_folder_mapping(folders):
    """  Builds a dictionary mapping columns to folders

    Args:
        folders (Datasource.folders_common): List of folder objects from the Datasource object

    Returns: A dict of column & folder mapping; i.e. {'column1': 'folderA', 'column2': 'folderA'}
    """
    mappings = dict()
    for folder in folders:
        if folder.folder_item:
            for item in folder.folder_item:
                field_name = item.name[1:-1]
                mappings[field_name] = folder.name
    return mappings


def generate_config(args, server: TableauServer = None):
    """ Downloads a datasource and saves configs for that datasource

    """

    # Set variables from the args
    definitions_csv_path = args.definitions_csv
    debugging_logs = args.debugging_logs
    location = args.location
    id = args.id
    datasource_name = args.name
    datasource_path = args.file_path
    project_name = args.project_name

    # Print Styling
    color = Color()
    symbol = Symbol()

    # Set file_prefix to false when this function is called from the merge_config namespace
    try:
        file_prefix = args.file_prefix
    except:
        file_prefix = False

    # Get the datasource name from the path
    if location == 'local':
        datasource_name = Path(datasource_path).stem
    # Download the datasouce and set values for
    elif location == 'online':
        obj = server.get_datasource(id, datasource_name, project_name)
        id = obj.id
        datasource_name = obj.name
        print(f'{color.fg_yellow}GETTING DATASOURCE {symbol.arrow_r} '
              f'{color.fg_grey}ID: {id} {symbol.sep} '
              f'NAME: {datasource_name} {symbol.sep} '
              f'INCLUDE EXTRACT: false{color.reset}')
        datasource_path = server.download_datasource(id, include_extract=False)

    print(f'{color.fg_yellow}BUILDING CONFIG {symbol.arrow_r} '
          f'{color.fg_grey}{datasource_name} {symbol.sep} {datasource_path}{color.reset}')
    datasource = Datasource(datasource_path)
    # Get column information from the metadata records
    metadata_record_config = get_metadata_record_config(
        datasource.connection.metadata_records,
        datasource_name,
        debugging_logs
    )

    # Get the mapping of definitions from the csv
    definitions_mapping = dict()
    if definitions_csv_path is not None:
        definitions_mapping = load_csv_with_definitions(file=definitions_csv_path)

    # Extract the columns and folders. Build the new config
    folder_mapping = build_folder_mapping(datasource.folders_common)
    column_configs, calculated_column_configs = create_column_config(
        columns=datasource.columns,
        datasource_name=datasource_name,
        folder_mapping=folder_mapping,
        metadata_record_columns=metadata_record_config,
        definitions_mapping=definitions_mapping,
        debugging_logs=debugging_logs
    )

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

    print(f'{color.fg_green}{symbol.success}  COLUMN CONFIG {symbol.arrow_r} '
          f'{color.fg_grey}{output_file_column_config}{color.reset}')
    print(f'{color.fg_green}{symbol.success}  CALCULATED COLUMN CONFIG {symbol.arrow_r} '
          f'{color.fg_grey}{output_file_calculated_column_config}{color.reset}')

    return output_file_column_config, output_file_calculated_column_config
