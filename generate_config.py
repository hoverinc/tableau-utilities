import argparse
import json
import os
import shutil
import xml.etree.ElementTree as ET

import yaml

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
    """  The config relies on a persona which is a

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

    # for persona_title, persona_details in personas.items():
    #
    #
    #     if persona_details['role'] == role and persona_details['role_type'] == role_type and persona_details['datatype'] == 'datatype':
    #         return persona(persona_title)


def create_column_config(columns, datasource_name):
    """ Generates a list of column configs with None for a folder

    ```{
      "Salesforce Opportunity Id": {
        "description": "The 18 digit account Id for a Salesforce opportunity",
        "folder": None,
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

        # Skip the table datatype for now
        if c['@datatype'] == 'table':
            pass
        else:
            persona = choose_persona(role=c['@role'], role_type=c['@type'], datatype=c['@datatype'])
            column_config = {
                c['@caption': {
                    "description": c['desc']['formatted-text']['run'],
                    "folder": None,
                    "persona": persona,
                    "datasources": [
                        {
                            "name": datasource_name,
                            "local-name": c['@name'],
                            "sql_alias": c['@name']
                        },
                    ]
                },
            }

            print(type(column_config))
            print(column_config)


def build_config(datasource, datasource_path):
    rows = dict()
    columns = [c.dict() for c in Datasource(datasource_path).columns]
    rows.setdefault(datasource.name, [])
    rows[datasource.name].extend(columns)

    create_column_config(columns=columns, datasource_name=datasource.name)

    # for c in columns:
    #     print(c)
    # if '@fiscal_year_start' in c:
    # # if c['@fiscal_uear_start'] == 'Salesforce Account Current Contract End Date':
    #     print(c)

    # create_column_config(columns)

    # folders = [c.dict() for c in Datasource(datasource_path).folders_common]
    # for f in folders:
    #     print(f)


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
