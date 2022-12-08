import argparse
import json
import os
import shutil
import xml.etree.ElementTree as ET
import yaml
from pprint import pprint


from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


# from tableau_utilities.general import convert_underscore_to_spaces_and_capitalize, get_datasource_files_dict


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
    parser.add_argument('--api_version', help='Tableau API version', default='2.8')
    parser.add_argument('--user', help='user name')
    parser.add_argument('--password', help='password')
    parser.add_argument('--token_secret', help='Personal Access Token Secret')
    parser.add_argument('--token_name', help='Personal Access Token Name')
    parser.add_argument('--datasource', help='The name of the datasources to generate a config for')
    parser.add_argument('--list_datasources', action='store_true',
                        help='Print the data sources and metadata about them to the console')
    return parser.parse_args()


def download_datasource(server, datasource_name=None, list_datasources=False):
    """ Downloads the specified datasources


    Args:
        server (TableauServer): A Tableau server object
        datasource_name: The name of the datasource to download
        list_datasources: Prints a sorted list of all the datasources from a site

    Returns:
        The path of the datasource to download
    """

    shutil.rmtree('tmp_tdsx', ignore_errors=True)
    tmp_folder = 'tmp_tdsx'
    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)
    datasource_list = [d for d in server.get_datasources()]

    sources = []
    for datasource in datasource_list:
        if list_datasources:
            info = {'name': datasource.name,
                    'project': datasource.project_name,
                    'connected_workbooks': datasource.connected_workbooks_count}
            sources.append(info)

        if datasource.name == datasource_name:
            datasource_path = server.download_datasource(datasource.id, include_extract=False)
            return datasource, datasource_path

    if list_datasources:
        sorted_sources = sorted(sources, key=lambda d: d['name'])
        for source in sorted_sources:
            print(source)


def choose_persona(role, role_type, datatype):
    # def choose_persona():
    """  The config relies on a persona which is a combination of role, role_type and datatype.
    This returns the persona name or raises an exception if the combination is not found

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
            # print(v['role'], v['role_type'], v['datatype'])
            # print(role, role_type, datatype)
            # print('-'*20)
            if role == v['role'] and role_type == v['role_type'] == role_type and datatype == v['datatype']:
                persona_name = k
                break

    if persona_name is not None:
        return persona_name
    else:
        raise ValueError(
            f"There is no persona for the combination of ROLE {role}, ROLE_TYPE {role_type}, and DATATYPE {datatype}'")


def create_column_config(columns, datasource_name, folder_mapping):
    """ Generates a list of column configs with None for a folder

    Args:
        columns
        datasource_name
        folder_mapping: A list of dictionaries mapping column name to folder name

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

    """

    column_configs = []

    for c in columns:

        column_name = c['@name'][1:-1]

        # Skip the table datatype for now
        if c['@datatype'] == 'table':
            pass
        else:
            persona = choose_persona(role=c['@role'], role_type=c['@type'], datatype=c['@datatype'])
            # column_config = {c['@caption': {}}

            description = None
            if 'desc' in c:
                description = c['desc']['formatted-text']['run']

            folder_name = None

            # for mapping in folder_mapping:
            #
            # # folder_name = [folder_mapping[column_name] for m in folder_mapping if column_name ==  ]
            #
            if column_name in folder_mapping.keys():
                folder_name = folder_mapping[column_name]

            column_config = {
                c['@caption']: {
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
                },
            }

            column_configs.append(column_config)

    return column_configs


def build_folder_mapping(datasource_path):
    folders = [c.dict() for c in Datasource(datasource_path).folders_common]

    mappings = {}
    for f in folders:
        folder_name = f['@name']
        print(type(f))
        print('-'*50)
        print(f)

        print(f)

        for item in f['folder-item']:
            # print(item)
            # print(folder_name, item)
            field_name = item['@name'][1:-1]
            mappings[field_name] = folder_name
            # field_and_folder = {field_name: folder_name}
            # mapping_list.append(field_and_folder)

    return mappings


def build_config(datasource, datasource_path):
    rows = dict()
    columns = [c.dict() for c in Datasource(datasource_path).columns]
    rows.setdefault(datasource.name, [])
    rows[datasource.name].extend(columns)

    # Build the folder mapping
    folder_mapping = build_folder_mapping(datasource_path)

    column_configs = create_column_config(columns=columns, datasource_name=datasource.name, folder_mapping=folder_mapping)
    print(column_configs)
    print(type(column_configs))

    for config in column_configs:
        pprint(config, sort_dicts=False, width=400, compact=False)
        # print(config)

    folder_mapping = build_folder_mapping(datasource_path)

    # for f in folder_mapping:
    #     print(f)

    for config in column_configs:
        pprint(config, sort_dicts=False)
        # print(config)

    # print(type(folder_mapping))

    # for f in folder_mapping:
    #     print(f)


    # for c in columns:
    #     print(c)
    # if '@fiscal_year_start' in c:
    # # if c['@fiscal_uear_start'] == 'Salesforce Account Current Contract End Date':
    #     print(c)

    # create_column_config(columns)

    # folders = [c.dict() for c in Datasource(datasource_path).folders_common]
    # for f in folders:
    #     # print(type(f))
    #     print('-'*50)
    #     # print(f)
    #
    #     for item in f['folder-item']:
    #         if item['@type'] == 'field':
    #             print('OH NO', item)
    #         # print(item)
    #
    # print(type(folders))


def generate_config(server, datasource_name):
    datasource, datasource_path = download_datasource(server, datasource_name)
    print(datasource_path)
    build_config(datasource, datasource_path)

    #     print("BUILDING CONFIG FOR:", datasource.project_name, (datasource.id, datasource.name))
    #     # datasource_path = server.download_datasource(datasource.id, include_extract=False)
    #
    #
    #     for c in columns:
    #         print(c)
    #
    #     folders = [c.dict() for c in Datasource(datasource_path).folders_common]
    #     for f in folders:
    #         print(f)
    #
    # else:
    #     print("SKIPPING:", datasource.project_name, (datasource.id, datasource.name))
    #

    # os.chdir('')
    # shutil.rmtree(tmp_folder)

    # return rows


if __name__ == '__main__':
    args = do_args()

    host = f'https://{args.server}.online.tableau.com'

    if args.user and args.password:
        ts = TableauServer(
            user=args.user,
            password=args.password,
            site=args.site,
            host=host,
            api_version=args.api_version
        )
    elif args.token_secret and args.token_name:
        ts = TableauServer(
            token_name=args.token_name,
            token_secret=args.token_secret,
            site=args.site,
            host=host,
            api_version=args.api_version
        )

    if args.list_datasources:
        download_datasource(ts, list_datasources=True)
    else:
        config = generate_config(ts, datasource_name=args.datasource)
    # with open('generated_config.json', 'w') as fd:
    #     json.dump(config, fd, indent=3)
