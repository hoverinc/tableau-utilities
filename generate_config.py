import json
import argparse
import os
import yaml
import shutil
import xml.etree.ElementTree as ET

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
    parser.add_argument('--list_datasources', help='Print the data sources and metadata about them to the console')
    return parser.parse_args()



def download_datasource(server, datasource_name, list_datasources=False):
    """ Gets a list of all columns in all datasources

    Args:
        server (TableauServer): A Tableau server object
    """
    shutil.rmtree('tmp_tdsx', ignore_errors=True)
    tmp_folder = 'tmp_tdsx'
    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)
    datasource_list = [d for d in server.get_datasources()]

    for datasource in datasource_list:
        if list_datasources:
            print(type(datasource))
            print(datasource)

        if datasource.name == datasource_name:
            datasource_path = server.download_datasource(datasource.id, include_extract=False)
            return datasource, datasource_path


def create_column_config(columns):
    print('-'*100)
    for c in columns:
        print(c)




def build_config(datasource, datasource_path):
    # tree = ET.parse(datasource_path)
    # root = tree.getroot()
    # for column in root.iter('column'):
    #     print(column)
    rows = dict()
    columns = [c.dict() for c in Datasource(datasource_path).columns]
    rows.setdefault(datasource.name, [])
    rows[datasource.name].extend(columns)

    for c in columns:
        if '@fiscal_year_start' in c:
        # if c['@fiscal_uear_start'] == 'Salesforce Account Current Contract End Date':
            print(c)

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


    config = generate_config(ts, datasource_name=args.datasource)
    # with open('generated_config.json', 'w') as fd:
    #     json.dump(config, fd, indent=3)
