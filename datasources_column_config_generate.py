import argparse
import json
import os
import shutil
from pprint import pprint

from tableau_utilities.tableau_server.tableau_server import TableauServer
from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_file.tableau_file_objects import MetadataRecord
from tableau_utilities.general.funcs import convert_to_snake_case


def do_args():
    """ Parse arguments.

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(description='List columns in Tableau datasources')
    parser.add_argument(
        '--server',
        required=True,
        help='Tableau Server URL. i.e. <server_address> in https://<server_address>.online.tableau.com',
        default=None
    )
    parser.add_argument(
        '--site',
        required=True,
        help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
        default=None)
    parser.add_argument('--api_version', help='Tableau API version', default='3.17')
    parser.add_argument('--user', help='user name')
    parser.add_argument('--password', help='password')
    parser.add_argument('--token_secret', help='Personal Access Token Secret')
    parser.add_argument('--token_name', help='Personal Access Token Name')
    parser.add_argument('--datasource', help='The name of the datasources to generate a config for')
    parser.add_argument('--clean_up_first', action='store_true', help='Deletes the directory and files before running')
    parser.add_argument('--folder_name', default='tmp_tdsx_and_config',  help='Specifies the folder to write the datasource and configs to')
    parser.add_argument('--file_prefix', action='store_true', help='Adds a prefix of the datasource name to the output file names')
    return parser.parse_args()


def download_datasource(server, datasource_name=None, list_datasources=False):
    """ Downloads the specified datasources


    Args:
        server (TableauServer): A Tableau server object
        datasource_name: The name of the datasource to download
        list_datasources: Prints a sorted list of all the datasources from a site

    Returns:
        datasource: The datasource object for the datasource that was downloaded
        datasource_path: The path of the datasource that was downloaded
    """
    datasource_list = [d for d in server.get_datasources()]

    sources = []
    for datasource in datasource_list:
        if datasource.name == datasource_name:
            datasource_path = server.download_datasource(datasource.id, include_extract=False)
            return datasource, datasource_path


def choose_persona(role, role_type, datatype):
    """  The config relies on a persona which is a combination of role, role_type and datatype for each column.
    This returns the persona name or raises an exception if the combination is not found

    Args:
        role: dimension or measure
        role_type: nominal, ordinal, or quantitative
        datatype: string, date, datetype, real, or boolean

    """

    personas = [
        {"string_dimension": {
            "role": "dimension",
            "role_type": "nominal",
            "datatype": "string"
        }},
        {"date_dimension": {
            "role": "dimension",
            "role_type": "ordinal",
            "datatype": "date"
        }},
        {"datetime_dimension": {
            "role": "dimension",
            "role_type": "ordinal",
            "datatype": "datetime"
        }},
        {"date_measure": {
            "role": "measure",
            "role_type": "ordinal",
            "datatype": "date"
        }},
        {"datetime_measure": {
            "role": "measure",
            "role_type": "ordinal",
            "datatype": "datetime"
        }},
        {"discrete_number_dimension": {
            "role": "dimension",
            "role_type": "ordinal",
            "datatype": "integer"
        }},
        {"continuous_number_dimension": {
            "role": "dimension",
            "role_type": "quantitative",
            "datatype": "integer"
        }},
        {"discrete_number_measure": {
            "role": "measure",
            "role_type": "ordinal",
            "datatype": "integer"
        }},
        {"continuous_number_measure": {
            "role": "measure",
            "role_type": "quantitative",
            "datatype": "integer"
        }},
        {"discrete_decimal_dimension": {
            "role": "dimension",
            "role_type": "ordinal",
            "datatype": "real"
        }},
        {"continuous_decimal_dimension": {
            "role": "dimension",
            "role_type": "quantitative",
            "datatype": "real"
        }},
        {"discrete_decimal_measure": {
            "role": "measure",
            "role_type": "ordinal",
            "datatype": "real"
        }},
        {"continuous_decimal_measure": {
            "role": "measure",
            "role_type": "quantitative",
            "datatype": "real"
        }},
        {"boolean_dimension": {
            "role": "dimension",
            "role_type": "nominal",
            "datatype": "boolean"
        }}
    ]

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


def get_metadata_record_columns(datasource_name, datasource, datasource_path):
    """

    """

    """ Builds a column config and caluclated field column config.  Writes each to individual files

    Args:
        datasource_name: The name of the datasource
        datasource: The datasoruce object
        datasource_path: The path to the of the datasource
        prefix: If true the output files are prefixed with the datasource name

    """

    rows = dict()
    metadata_records = [c.dict() for c in Datasource(datasource_path).connection.metadata_records]
    rows.setdefault(datasource.name, [])
    rows[datasource.name].extend(metadata_records)

    metadata_record_columns = {}

    for m in metadata_records:
        if m['@class'] == 'column':

        # For these we don't have all the information needed to assign a persona. We'll assume they are dimensions
        # as that is a safer plan instead of measures where bad math can be done
            if m["local-type"] == 'string':
                persona = 'string_dimension'
            elif m["local-type"] == 'date':
                persona = 'date_dimension'
            elif m["local-type"] == 'datetime':
                persona = 'datetime_dimension'
            elif m["local-type"] == 'boolean':
                persona = 'boolean_dimension'
            elif m["local-type"] == 'integer':
                persona = 'discrete_number_dimension'
            elif m["local-type"] == 'real':
                persona = 'continuous_decimal_dimension'

            metadata_record_columns[m['remote-name']] = {'persona': persona,
            "datasources": [
                        {
                            "name": datasource_name,
                            "local-name": m['local-name'][1:-1],
                            "sql_alias": m['remote-name']
                        },
                ]

            }

    return metadata_record_columns


def create_column_config(columns, datasource_name, folder_mapping, metadata_record_columns):
    """ Generates a list of column configs with None for a folder

    Args:
        columns
        datasource_name
        folder_mapping: A list of dictionaries mapping column name to folder name
        metadata_record_columns

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
        column_name_list.append(column_name)

        # Make a title case caption from the database name if there is no caption
        if '@caption' in 'c':
            caption = c['@caption']
        else:
            caption = column_name.replace('_', ' ').title()

        # Skip the table datatype for now
        if c['@datatype'] == 'table':
            pass
        else:
            persona = choose_persona(role=c['@role'], role_type=c['@type'], datatype=c['@datatype'])

            description = ''
            if 'desc' in c:
                description = c['desc']['formatted-text']['run']

            folder_name = None
            if column_name in folder_mapping.keys():
                folder_name = folder_mapping[column_name]

            # Calculations are written to a separate config in the Airflow DAG
            if 'calculation' in c:

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

            else:

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

    # Add column configs for metadata_record columns when there wasn't a column object in the above coce

    for k, v in metadata_record_columns.items():
        if k in column_name_list:
            pass
        else:
            caption = k.replace('_', ' ').title()

            # if v['persona'] in ['string_dimension', 'date_dimension', 'datetime_dimension', 'boolean_dimension', 'discrete_number_dimension']:

            column_configs[caption] = {
                "description": '',
                "folder": None,
                "persona": v['persona'],
                "datasources": v['datasources']
            }

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


def build_config(datasource_name, datasource, datasource_path, metadata_record_columns, prefix):
    """ Builds a column config and caluclated field column config.  Writes each to individual files

    Args:
        datasource_name: The name of the datasource
        datasource: The datasoruce object
        datasource_path: The path to the of the datasource
        metadata_record_columns: The columns from the metadata records
        prefix: If true the output files are prefixed with the datasource name

    """

    rows = dict()
    columns = [c.dict() for c in Datasource(datasource_path).columns]
    rows.setdefault(datasource.name, [])
    rows[datasource.name].extend(columns)

    # Build the folder mapping
    folder_mapping = build_folder_mapping(datasource_path)

    column_configs, calculated_column_configs = create_column_config(columns=columns,
                                                                     datasource_name=datasource.name,
                                                                     folder_mapping=folder_mapping,
                                                                     metadata_record_columns=metadata_record_columns)

    print('-' * 20, 'COLUMN CONFIG', '-' * 20)
    for config in column_configs:
        pprint(config, sort_dicts=False, width=200)

    print('-'*20, 'CALCULATED FIELD COLUMN CONFIG', '-'*20)
    for config in calculated_column_configs:
        pprint(config, sort_dicts=False, width=200)

    datasource_name_snake = convert_to_snake_case(datasource_name)
    output_file_column_config = 'column_config.json'
    output_file_calculated_column_config = 'tableau_calc_config.json'

    if prefix:
        output_file_column_config = f'{datasource_name_snake}__{output_file_column_config}'
        output_file_calculated_column_config = f'{datasource_name_snake}__{output_file_calculated_column_config}'

    with open(output_file_column_config, "w") as outfile:
        json.dump(column_configs, outfile)

    with open(output_file_calculated_column_config, "w") as outfile:
        json.dump(calculated_column_configs, outfile)

    print('DATSOURCE PATH:', datasource_path)
    print('COLUMN CONFIG PATH:', datasource_path)
    print('CALCULATED COLUMN CONFIG PATH:', datasource_path)


def generate_config(server, datasource_name, prefix=False):
    """ Downloads a datasource and saves configs for that datasource

    Args:
        server: the tableau server authentication
        datasource_name: The name of the datasource to generate the config for
        prefix: If true the configs will have the datasource name as a prefix

    """

    datasource, datasource_path = download_datasource(server, datasource_name)
    metadata_record_columns = get_metadata_record_columns(datasource_name, datasource, datasource_path)
    build_config(datasource_name, datasource, datasource_path, metadata_record_columns, prefix)


if __name__ == '__main__':
    args = do_args()

    host = f'https://{args.server}.online.tableau.com'

    ts = TableauServer(
        personal_access_token_name=args.token_name,
        personal_access_token_secret=args.token_secret,
        user=args.user,
        password=args.password,
        site=args.site,
        host=host,
        api_version=args.api_version
    )

    tmp_folder = args.folder_name
    if args.clean_up_first:
        shutil.rmtree(args.folder_name, ignore_errors=True)

    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)

    if args.file_prefix:
        add_prefix = True
    else:
        add_prefix = False

    generate_config(ts, datasource_name=args.datasource, prefix=add_prefix)
