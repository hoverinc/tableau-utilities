from argparse import RawTextHelpFormatter
import argparse
import json
import os
import shutil
import sys
from pprint import pprint

from tableau_utilities.tableau_server.tableau_server import TableauServer
from tableau_utilities.scripts.datasources_column_config_generate import main
from tableau_utilities.scripts.gen_config import generate_config



def do_args():
    """ Parse arguments.

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(description='Tableau Utilities CLI:\n'
                                                 '-Manage Tableau Server/Online\n'
                                                 '-Manage configurations to edit datasource metadata',
                                     formatter_class=RawTextHelpFormatter)
    parser = argparse.ArgumentParser(prog='tableau_utilities')
    subparsers = parser.add_subparsers(help='You must choose a script area to run',  required=True)

    group_server = parser.add_argument_group('server_information', 'Server Information')
    group_server.add_argument(
        '--server',
        help='Tableau Server URL. i.e. <server_address> in https://<server_address>.online.tableau.com',
        default=None
    )
    group_server.add_argument(
        '--site',
        help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
        default=None)
    group_server.add_argument('--api_version', help='Tableau API version', default='3.17')

    group_user_password = parser.add_argument_group('user_pass', 'Authentication with username and password method')
    group_user_password.add_argument('--user', help='user name')
    group_user_password.add_argument('--password', help='password')

    group_token = parser.add_argument_group('token_info', 'Authentication with a Personal Access Token (PAT)')
    group_token.add_argument('--token_secret', help='Personal Access Token Secret')
    group_token.add_argument('--token_name', help='Personal Access Token Name')

    # SERVER INFO
    parser_server_info = subparsers.add_parser('server_info', help='Retrieve and view information from Tableau Cloud/Server')

    # GENERATE CONFIG
    parser_config_gen = subparsers.add_parser('generate_config', help='Generate configs to programatically manage metdatadata in Tableau datasources via Airflow')
    parser_config_gen.add_argument('--datasource', help='The name of the datasources to generate a config for')
    parser_config_gen.add_argument('--clean_up_first', action='store_true', help='Deletes the directory and files before running')
    parser_config_gen.add_argument('--folder_name', default='tmp_tdsx_and_config',
                        help='Specifies the folder to write the datasource and configs to')
    parser_config_gen.add_argument('--file_prefix', action='store_true',
                        help='Adds a prefix of the datasource name to the output file names')
    parser_config_gen.add_argument('--definitions_csv',
                        help='Allows a csv with definitions to be inputted for adding definitions to a config. It may be easier to populate definitions in a spreadsheet than in the configo ')
    parser_config_gen.set_defaults(func=generate_config)

    # MERGE CONFIG
    parser_config_merge = subparsers.add_parser('merge_config', help='Merge a new config into the existing master config')
    parser_config_merge.add_argument('-e', '--existing_config',
                        help='The path to the current configuration')
    parser_config_merge.add_argument('-a', '--additional_config',
                        help='The path to the configuration. This code ASSUMES that the additional config is for a single datasource ')
    parser_config_merge.add_argument('-n', '--merged_config', default='merged_config',
                        help='The name of the merged config JSON file.  For my_config.json enter my_config. Do not enter the .json extension')
    parser_config_merge.add_argument('-f', '--folder_name', default='tmp_tdsx_and_config',
                        help='Specifies the folder to write the datasource and configs to')

    return parser.parse_args()


def main():
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

    # Passes the TS object into all functions even though it's not always needed
    args.func(args, ts)



if __name__ == '__main__':
    main()



