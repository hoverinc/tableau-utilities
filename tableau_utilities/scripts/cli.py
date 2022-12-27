from argparse import RawTextHelpFormatter
import argparse
import json
import os
import shutil
import sys
from pprint import pprint

from tableau_utilities.tableau_server.tableau_server import TableauServer
# from tableau_utilities.scripts.datasources_column_config_generate import main
from tableau_utilities.scripts.gen_config import generate_config
from tableau_utilities.scripts.merge_config import merge_configs
from tableau_utilities.scripts.server_info import server_info
from tableau_utilities.scripts.download_publish import download_objects


def do_args():
    """ Parse arguments.

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(description='Tableau Utilities CLI:\n'
                                                 '-Manage Tableau Server/Online\n'
                                                 '-Manage configurations to edit datasource metadata',
                                     formatter_class=RawTextHelpFormatter)
    parser = argparse.ArgumentParser(prog='tableau_utilities')
    subparsers = parser.add_subparsers(title="commands", dest="command", help='You must choose a script area to run', required=True)

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

    group_local_folder = parser.add_argument_group('local_folder', 'Manage where to read/write files')
    group_local_folder.add_argument('--folder_name', default='tmp_tdsx_and_config',
                        help='Specifies the folder to write the datasource and configs to')
    group_local_folder.add_argument('--clean_up_first', action='store_true', help='Deletes the directory and files before running')

    # SERVER INFO
    parser_server_info = subparsers.add_parser('server_info',
                                               help='Retrieve and view information from Tableau Cloud/Server')
    parser_server_info.add_argument('--list_object', choices=['datasource', 'project', 'workbook'], help='List information about the Object')
    parser_server_info.add_argument('--list_verbosity', choices=['names', 'names_ids', 'ids_names', 'full_df', 'full_dictionary', 'full_dictionary_pretty'],
                                   help='Set the amount of information and the format to display')
    parser_server_info.add_argument('--list_sort_field', default='name',
                                   help='Set the amount of information and the format to display')
    parser_server_info.set_defaults(func=server_info)

    # DOWNLOAD & PUBLISH
    parser_server_download = subparsers.add_parser('server_download_publish',
                                               help='Retrieve and view objects from Tableau Cloud/Server')
    parser_server_download.add_argument('--object_type', choices=['datasource', 'workbook'],
                                    help='List information about the Object')
    parser_server_download.add_argument('--id', help='Set the amount of information and the format to display')
    parser_server_download.add_argument('--name',  help='The datasource or workbook name')
    parser_server_download.add_argument('--project_name', help='The project name for the datasource or workbook')
    parser_server_download.add_argument('--include_extract', action='store_true',
                                        help='includes the extract in the download if specified. This will make downloads take a long time for large extracts')
    parser_server_download.set_defaults(func=download_objects)

    # GENERATE CONFIG
    parser_config_gen = subparsers.add_parser('generate_config',
                                              help='Generate configs to programatically manage metdatadata in Tableau datasources via Airflow')
    parser_config_gen.add_argument('--datasource', help='The name of the datasources to generate a config for')
    parser_config_gen.add_argument('--file_prefix', action='store_true',
                        help='Adds a prefix of the datasource name to the output file names')
    parser_config_gen.add_argument('--definitions_csv',
                        help='Allows a csv with definitions to be inputted for adding definitions to a config. It may be easier to populate definitions in a spreadsheet than in the configo ')
    parser_config_gen.set_defaults(func=generate_config)

    # MERGE CONFIG
    parser_config_merge = subparsers.add_parser('merge_config',
                                                help='Merge a new config into the existing master config')
    parser_config_merge.add_argument('-ec', '--existing_config',
                        help='The path to the current configuration')
    parser_config_merge.add_argument('-ac', '--additional_config',
                        help='The path to the configuration. This code ASSUMES that the additional config is for a single datasource ')
    parser_config_merge.add_argument('-mc', '--merged_config', default='merged_config',
                        help='The name of the merged config JSON file.  For my_config.json enter my_config. Do not enter the .json extension')
    parser_config_merge.set_defaults(func=merge_configs)

    return parser.parse_args()


def main():
    args = do_args()

    print(args.server, args.site, args.token_secret, args.token_name)

    site = os.getenv("TABLEAU_SITENAME_TEST")
    server = os.getenv("TABLEAU_SERVER_ADDRESS_TEST")
    token_name = os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_NAME_TEST")
    token_secret = os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_VALUE_TEST")

    # Set/Reset the directory
    tmp_folder = args.folder_name
    if args.clean_up_first:
        shutil.rmtree(tmp_folder, ignore_errors=True)

    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)

    needs_tableau_server = (
        args.command == 'generate_config'
        or args.command == 'server_info'
        or args.command == 'server_download'
    )

    # Create the server object and run the functions
    if needs_tableau_server:
        host = f'https://{server}.online.tableau.com'
        ts = TableauServer(
            personal_access_token_name=token_name,
            personal_access_token_secret=token_secret,
            user=args.user,
            password=args.password,
            site=site,
            host=host,
            api_version=args.api_version
        )
        args.func(args, ts)

    # Run functions that don't need the server
    else:
        args.func(args)


if __name__ == '__main__':
    main()



