import argparse
import os
import shutil
from argparse import RawTextHelpFormatter
import yaml

from tableau_utilities.tableau_server.tableau_server import TableauServer

from tableau_utilities.scripts.gen_config import generate_config
from tableau_utilities.scripts.merge_config import merge_configs
from tableau_utilities.scripts.server_info import server_info
from tableau_utilities.scripts.server_operate import server_operate
from tableau_utilities.scripts.connection import connection
from tableau_utilities.scripts.datasource import datasource
from tableau_utilities.scripts.csv_config import csv_config

parser = argparse.ArgumentParser(prog='tableau_utilities',
                                 description='Tableau Utilities CLI:\n'
                                             '-Manage Tableau Server/Online\n'
                                             '-Manage configurations to edit datasource metadata',
                                 formatter_class=RawTextHelpFormatter)
subparsers = parser.add_subparsers(title="commands", dest="command", help='You must choose a command.',
                                   required=True)
parser.add_argument('-a', '--auth', choices=['settings_yaml', 'args_user_pass', 'args_token', 'os_env'],
                    help='The method for storing your credentials to pass into the CLI.')
parser.add_argument('-d', '--debugging_logs', action='store_true',
                    help='Print detailed logging to the console to debug CLI')

# GROUP: SERVER INFORMATION
group_server = parser.add_argument_group('server_information', 'Server Information')
group_server.add_argument(
    '-s', '--server',
    help='Tableau Server URL. i.e. <server_address> in https://<server_address>.online.tableau.com',
    default=None
)
group_server.add_argument(
    '-sn', '--site_name',
    help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
    default=None)
group_server.add_argument('--api_version', help='Tableau API version', default='3.17')

# GROUP: USER & PASSWORD
group_user_password = parser.add_argument_group('user_pass', 'Authentication with username and password method')
group_user_password.add_argument('-u', '--user', help='user name')
group_user_password.add_argument('-p', '--password', help='password')

# GROUP: PERSONAL ACCESS TOKENS
group_token = parser.add_argument_group('token_info', 'Authentication with a Personal Access Token (PAT)')
group_token.add_argument('-ts', '--token_secret', help='Personal Access Token Secret')
group_token.add_argument('-tn', '--token_name', help='Personal Access Token Name')

# GROUP: SETTINGS YAML
group_settings_yaml = parser.add_argument_group('settings.yaml', 'Authentication with settings in a .yaml file')
group_settings_yaml.add_argument('--settings_path',
                                 help='Path to your local settings.yaml file (See sample_settings.yaml)'
                                 )

# GROUP: LOCAL FOLDER
group_local_folder = parser.add_argument_group('local_folder', 'Manage where to read/write files')
group_local_folder.add_argument('--local_folder', default='tmp_tdsx_and_config',
                                help='Specifies the folder to write the datasource and configs to')
group_local_folder.add_argument('--clean_up_first', action='store_true',
                                help='Deletes the directory and files before running')

# GROUP: File Information
group_file = parser.add_argument_group('file',
                                             'Information for datasource operations such as generate_config and update_connection')
group_file.add_argument('-l', '--location', choices=['local', 'online'],
                               help='Specify the location of the datasource or workbook')
group_file.add_argument('-i', '--id', help='The ID for the object on Tableau Cloud/Server')
group_file.add_argument('-n', '--name', help='The datasource or workbook name in Tableau Cloud/Server Use with --project_name.')
group_file.add_argument('-pn', '--project_name',
                                   help='The project name for the datasource or workbook in Tableau Cloud/Server Use with --name.')
group_file.add_argument('-f', '--file_path', help='The path to the file to publish or interact with')
group_file.add_argument('--definitions_csv',
                        help='Add data defintions from a csv to the config. It may be easier to bulk '
                                    'populate definitions in a spreadsheet than in the config.')
group_file.add_argument('--include_extract', action='store_true',
                                   help='Includes the extract in the download if specified. '
                                        'This will make downloads take a long time for large extracts.')

# SERVER INFO
parser_server_info = subparsers.add_parser('server_info',
                                           help='Retrieve and view information from Tableau Cloud/Server')
parser_server_info.add_argument('-lo', '--list_object', choices=['datasource', 'project', 'workbook'], required=True,
                                help='Specify the type of object for the information.')
parser_server_info.add_argument('-lf', '--list_format', default='names',
                                choices=['names', 'names_ids', 'names_projects', 'ids_names', 'full_df',
                                         'full_dictionary',
                                         'full_dictionary_pretty'],
                                help='Set the fields and format for the information.')
parser_server_info.add_argument('-lsf', '--list_sort_field', default='name',
                                help='Choose the field for sorting the information.')
parser_server_info.set_defaults(func=server_info)

# SERVER OPERATE
parser_server_operate = subparsers.add_parser('server_operate',
                                              help='Download, publish, and refresh objects on Tableau Cloud/Server')
parser_server_operate.add_argument('--action_type', choices=['download', 'publish', 'refresh'], required=True,
                                   help='List information about the Object')
parser_server_operate.add_argument('--object_type', choices=['datasource', 'workbook'], required=True,
                                   help='List information about the Object')
parser_server_operate.add_argument('--all',  action='store_true', help='Download all workbooks or datasources')
parser_server_operate.set_defaults(func=server_operate)

# UPDATE CONNECTION
parser_connection = subparsers.add_parser('connection',
                                          help='Update connection for a datasource or embed a username and password.\n'
                                               'Requires datasource arguments.')
parser_connection.add_argument('--connection_operation', choices=['update_local_connection', 'embed_user_pass'], required=True,
                               help='Specify the location of the datasource.')
parser_connection.add_argument('-c', '--conn', choices=['settings_yaml', 'args', 'os_env'],
                               help='The method for storing the connection credentials to pass into the cli.')
parser_connection.add_argument('--conn_user', default=None, help='Username for embed credentials. See --embed_creds.')
parser_connection.add_argument('--conn_pw', default=None, help='Password for embed credentials. See --embed_creds')
parser_connection.add_argument('--conn_type', default='snowflake',
                               help='Connection type for embed credentials. See --embed_creds')
parser_connection.add_argument('--conn_db', default=None, help='Connection Database')
parser_connection.add_argument('--conn_schema', default=None, help='Connection Schema')
parser_connection.add_argument('--conn_host', default=None, help='Connection Host (URL)')
parser_connection.add_argument('--conn_role', default=None, help='Connection Role')
parser_connection.add_argument('--conn_warehouse', default=None, help='Connection Warehouse')
parser_connection.set_defaults(func=connection)

# DATASOURCE
parser_datasource = subparsers.add_parser('datasource', help='View and edit metadata about the datasource')
parser_datasource.add_argument('-tds', '--save_tds', action='store_true',
                               help='Saves the TDS for the datasource to view the raw xml')
parser_datasource.add_argument('--folder', choices=['add', 'delete'],
                               help='Add or delete a folder in the downloaded datasource')
parser_datasource.add_argument(
    '--folder_name',
    default=None,
    help='The name of the folder. See --add_column, --modify_column, --add_folder and --delete_folder'
)
parser_datasource.add_argument('--column', choices=['add', 'modify'],
                               help='Add or modify a column in the downloaded datasource')
parser_datasource.add_argument('--column_name', help='The local name of the column. Required.')
parser_datasource.add_argument('--remote_name', help='The remote (SQL) name of the column.')
parser_datasource.add_argument('--caption', help='Short name/Alias for the column')
parser_datasource.add_argument('--title_case_caption', default=False, action='store_true',
                               help='Converts caption to title case. Applied after --caption')
parser_datasource.add_argument('--role', default=None, choices=['measure', 'dimension'],
                               help='required for adding a new column')
parser_datasource.add_argument('--desc', help='A Tableau column description')
parser_datasource.add_argument('--calculation', help='A Tableau calculation')
parser_datasource.add_argument('--datatype', default=None, choices=['date', 'datetime', 'integer', 'real', 'string'],
                               help='required for adding a new column')
parser_datasource.add_argument('--role_type', default=None, choices=['nominal', 'ordinal', 'quantitative'],
                               help='required for adding a new column')
parser_datasource.set_defaults(func=datasource)

# GENERATE CONFIG
parser_config_gen = subparsers.add_parser('generate_config',
                                          help='Generate configs to programatically manage metdatadata in Tableau '
                                               'datasources via Airflow.\nRequires datasource arguments.\n')
parser_config_gen.add_argument('--file_prefix', action='store_true',
                               help='Adds a prefix of the datasource name to the output file names.')
parser_config_gen.set_defaults(func=generate_config)

# CONFIG TO CSV
parser_config_csv = subparsers.add_parser('csv_config',
                                          help='Write a config to a csv with 1 row per column per datasource')
parser_config_csv.add_argument('-cl', '--config_list', nargs='+', help='The list of paths to the configs')
parser_config_csv.set_defaults(func=csv_config)


# MERGE CONFIG
parser_config_merge = subparsers.add_parser('merge_config',
                                            help='Merge a new config into the existing master config',
                                            formatter_class=RawTextHelpFormatter)
parser_config_merge.add_argument('-mw', '--merge_with', choices=['config', 'csv', 'generate_merge_all'],
                                 help='config: merge an existing config with a new config\n'
                                      'csv: merge an existing config with a csv containing data definitions\n'
                                      'generate_merge_all: Runs generate_config to create a column_config and a calc config from a datasource. '
                                      'Merges both with existing configs in a target directory')
parser_config_merge.add_argument('-e', '--existing_config',
                                 help='The path to the current configuration. The current configuration may have '
                                      'more than 1 datasource.')
parser_config_merge.add_argument('-a', '--additional_config',
                                 help='The path to the configuration to add. This code ASSUMES that the additional '
                                      'config is for a single datasource ')
parser_config_merge.add_argument('-m', '--merged_config', default='merged_config',
                                 help='The path of the file to write the merged JSON config file. '
                                      'Ex: filename "my_config.json" '
                                      'argument is "my_config" Do not enter the .json extension.')
parser_config_merge.add_argument('-td', '--target_directory', default='merged_config',
                                 help='The path containing the existing configs. Use with --merge_with generate_merge_all')

parser_config_merge.set_defaults(func=merge_configs)


def validate_args_server_operate(args):
    """ Validate that combinations of args are present
    """
    if (args.name and not args.project_name) or (args.project_name and not args.name):
        parser.error('--name and --project_name are required together')

    if args.action_type == 'publish' and (args.name is None or args.project_name is None or args.file_path is None):
        parser.error('publish requires a --name, --project_name, and --file_path ')


def validate_args_id_name_project(args):
    """ Validate that combinations of args are present

    """

    if args.location is None:
        parser.error(f'The {args.command} command requires --location')

    if args.location == 'local' and not args.file_path:
        parser.error('--location local requires a --file_path')

    if args.location == 'online':
        if args.id:
            pass
        elif (args.name and not args.project_name) or (
                args.project_name and not args.name):
            parser.error('--name and --project_name are required together')
        elif not args.id and not args.name and not args.project_name:
            parser.error(
                '--location online requires either a --id or a --name and --project_name')


def validate_auth_included(args):
    """ Validates that auth is included for operations that need it and that the parameters are present

    """

    if not args.auth:
        parser.error('These commands required --auth method and credentials to authenticate with Tableau Server/Online')
    if args.auth == 'args_user_pass':
        if not args.user or not args.password:
            parser.error('You must include --user and --password for args_user_pass authentication')
    if args.auth == 'args_token':
        if not args.token_name or not args.token_secret:
            parser.error('You must include --token_name and --token_secret for args_token authentication')
    if args.auth == 'settings_yaml' and not args.settings_path:
        parser.error('You must include --settings_path for settings_yaml authentication')


def validate_args_command_connection(args):
    """ Validate connection args are incluedd

    """

    if args.conn is None:
        parser.error(f'{args.command} requires --conn')


def validate_args_command_datasource(args):
    """ Validates args for the datasource command

    """

    if args.column:
        if args.file_path is None:
            parser.error(f'{args.command} --column requires --file_path')


def validate_args_command_merge_config(args):
    if args.merge_with in ('config', 'csv') and args.existing_config is None:
        parser.error(f'--merge_with {args.merge_with} requires --existing_config')
    if args.merge_with == 'csv' and args.definitions_csv is None:
        parser.error(f'--merge_with {args.merge_with} requires --definitions_csv')
    if args.merge_with == 'config' and args.additional_config is None:
        parser.error(f'--merge_with {args.merge_with} requires --existing_config')
    if args.merge_with == 'generate_merge_all' and args.target_directory is None:
        parser.error(f'--merge_with {args.merge_with} requires --target_directory')


def tableau_authentication(args):
    """ Creates the Tableau server authentication from a variety of methods for passing in credentials

    """

    # Set the defaults from the args
    user = args.user
    password = args.password
    token_name = args.token_name
    token_secret = args.token_secret
    site = args.site_name
    server = args.server
    api_version = args.api_version

    if args.auth in ['args_user_pass', 'args_token']:
        print('Using auth from the args passed in')
    # Override the defaults with information from a settings file
    elif args.auth == 'settings_yaml':
        print('Using auth from the settings yaml')
        with open(args.settings_path, 'r') as f:
            settings = yaml.safe_load(f)

        site = settings['tableau_login']['site']
        server = settings['tableau_login']['server']
        token_name = settings['tableau_login']['token_name']
        token_secret = settings['tableau_login']['token_secret']
        api_version = settings['tableau_login']['api_version']
        user = settings['tableau_login']['user']
        password = settings['tableau_login']['password']
    # Override the defaults with information from the OS
    elif args.auth == 'os_env':
        print('Using auth OS environment')
        site = os.getenv("TABLEAU_SITENAME")
        server = os.getenv("TABLEAU_SERVER_ADDRESS")
        token_name = os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_NAME")
        token_secret = os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_VALUE")
        api_version = args.api_version

    # Validate the combinations for authentication methods
    if (token_secret and not token_name) or (token_name and not token_secret):
        parser.error('--token_secret and --token_name are required together')

    if (user and not password) or (password and not user):
        parser.error('--password and --user are required together')

    # Create the server object and run the functions
    host = f'https://{server}.online.tableau.com'
    ts = TableauServer(
        personal_access_token_name=token_name,
        personal_access_token_secret=token_secret,
        user=user,
        password=password,
        site=site,
        host=host,
        api_version=api_version
    )

    return ts


def main():
    args = parser.parse_args()

    # Validate the arguments
    if args.command == 'server_operate':
        validate_args_server_operate(args)
    if args.command in ['generate_config', 'connection']:
        validate_args_id_name_project(args)
    if args.command == 'connection':
        validate_args_command_connection(args)
    if args.command == 'datasource':
        validate_args_command_datasource(args)
    if args.command == 'merge_config':
        validate_args_command_merge_config(args)

    # Set/Reset the directory
    tmp_folder = args.local_folder
    if args.clean_up_first:
        shutil.rmtree(tmp_folder, ignore_errors=True)

    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)

    needs_tableau_server = (
            (args.command == 'generate_config' and args.location == 'online')
            or (args.command == 'merge_config' and args.location == 'online')
            or (args.command == 'datasource' and args.location == 'online')
            or (args.command == 'connection' and args.connection_operation == 'embed_user_pass')
            or args.command == 'server_info'
            or args.command == 'server_operate'
    )

    if needs_tableau_server:
        print("Tableau Server Operations")
        validate_auth_included(args)
        ts = tableau_authentication(args)
        args.func(args, ts)

    # Run functions that don't need the server
    else:
        print('Local Operations')
        args.func(args)


if __name__ == '__main__':
    main()
