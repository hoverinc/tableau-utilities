import argparse
import json
import os
import shutil
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


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
    parser.add_argument('--merge', action='store_true',
                        help='Merges config B into config A. Adds new config items. If there are conflicts '
                             'it will assume config B in the correct information.')
    return parser.parse_args()


def build_config(datasource, datasource_path):
    """ Builds a column config and caluclated field column config.  Writes each to individual files

    Args:
        datasource: The datasoruce object
        datasource_path: The name of the datasource

    """

    rows = dict()
    columns = [c.dict() for c in Datasource(datasource_path).columns]
    rows.setdefault(datasource.name, [])
    rows[datasource.name].extend(columns)

    # Build the folder mapping
    folder_mapping = build_folder_mapping(datasource_path)

    column_configs, calculated_column_configs = create_column_config(columns=columns,
                                                                     datasource_name=datasource.name,
                                                                     folder_mapping=folder_mapping)

    print('-' * 20, 'COLUMN CONFIG', '-' * 20)
    for config in column_configs:
        pprint(config, sort_dicts=False, width=200)

    print('-'*20, 'CALCULATED FIELD COLUMN CONFIG', '-'*20)
    for config in calculated_column_configs:
        pprint(config, sort_dicts=False, width=200)

    with open("column_config.json", "w") as outfile:
        json.dump(column_configs, outfile)

    with open("tableau_calc_config.json", "w") as outfile:
        json.dump(calculated_column_configs, outfile)

    print('DATSOURCE PATH:', datasource_path)
    print('COLUMN CONFIG PATH:', datasource_path)
    print('CALCULATED COLUMN CONFIG PATH:', datasource_path)


def generate_config(server, datasource_name):

    shutil.rmtree('tmp_tdsx', ignore_errors=True)
    tmp_folder = 'tmp_tdsx'
    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)

    datasource, datasource_path = download_datasource(server, datasource_name)
    build_config(datasource, datasource_path)


if __name__ == '__main__':
    args = do_args()

    if ('LoadFiles' in vars(args) and
            'SourceFolder' not in vars(args) and
            'SourceFile' not in vars(args)):
        pass

    if args.server:
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

    if args.list_datasources:
        download_datasource(ts, list_datasources=True)
    else:
        generate_config(ts, datasource_name=args.datasource)
