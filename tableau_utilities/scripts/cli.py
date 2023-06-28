import argparse
import os
import shutil
from argparse import RawTextHelpFormatter
import yaml
import pkg_resources

import tableau_utilities.tableau_server.tableau_server as ts

from tableau_utilities.general.config_column_persona import personas
from tableau_utilities.general.cli_styling import Color, Symbol, color_print
from tableau_utilities.scripts.gen_config import generate_config
from tableau_utilities.scripts.merge_config import merge_configs
from tableau_utilities.scripts.server_info import server_info
from tableau_utilities.scripts.server_operate import server_operate
from tableau_utilities.scripts.datasource import datasource
from tableau_utilities.scripts.csv_config import csv_config

__version__ = pkg_resources.require("tableau_utilities")[0].version

parser = argparse.ArgumentParser(
    prog='tableau_utilities',
    description='Tableau Utilities CLI:\n'
                '-Manage Tableau Server/Online\n'
                '-Manage configurations to edit datasource metadata',
    formatter_class=RawTextHelpFormatter
)
parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}',
                    help='Print the current version of the CLI')
parser.add_argument('-d', '--debugging_logs', action='store_true',
                    help='Print detailed logging to the console to debug CLI')
subparsers = parser.add_subparsers(title="commands", dest="command", help='You must choose a command.', required=True)

# GROUP: Tableau Server Authentication
group_server_auth = parser.add_argument_group(
    'server_auth', 'Authentication used when interacting with Tableau Server'
)
group_server_auth.add_argument(
    '-s', '--server',
    help='Tableau Server URL. i.e. <server_address> in https://<server_address>.online.tableau.com'
)
group_server_auth.add_argument(
    '-sn', '--site_name',
    help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>'
)
group_server_auth.add_argument('--api_version', help='Tableau API version')
group_server_auth.add_argument('-u', '--user', help='The Tableau Server Username. Must pair with --password')
group_server_auth.add_argument('-p', '--password', help='The Tableau Server Password. Must pair with --user')
group_server_auth.add_argument('-ts', '--token_secret',
                               help='The Tableau Server Personal Access Token Secret. Must pair with --token_name')
group_server_auth.add_argument('-tn', '--token_name',
                               help='The Tableau Server Personal Access Token Name. Must pair with --token_secret')

# GROUP: SETTINGS YAML
group_settings_yaml = parser.add_argument_group('settings.yaml', 'Authentication with settings in a .yaml file')
group_settings_yaml.add_argument('--settings_path', default='settings.yaml',
                                 help='Path to your local settings.yaml file (See sample_settings.yaml)')

# GROUP: Output Directory
group_output_dir = parser.add_argument_group(
    'output_dir',
    'Manage the local directory where files will read from and write to'
)
group_output_dir.add_argument('-o', '--output_dir', default='tmp_tdsx_and_config',
                              help='Specifies the folder to write the datasource and configs to')
group_output_dir.add_argument('-c', '--clean_dir', action='store_true',
                              help='Deletes the directory, and all files within, before running')

# GROUP: File Information
group_file = parser.add_argument_group(
    'file', 'Args for commands that require specific information for a Tableau File; '
            'i.e. datasource, generate_config, or merge_config'
)
group_file.add_argument('-l', '--location', choices=['local', 'online'],
                        help='Specify the location of the Tableau File Object.')
group_file.add_argument('-i', '--id', help='The ID for the Tableau File Object in Cloud/Server.')
group_file.add_argument('-n', '--name',
                        help='The name of the Tableau File Object in Cloud/Server; Use with --project_name.')
group_file.add_argument('-pn', '--project_name',
                        help='The project name for the Tableau File Object in Cloud/Server; Use with --name.')
group_file.add_argument('-f', '--file_path', help='The path to the local Tableau File.')
group_file.add_argument('--definitions_csv',
                        help='Path to a CSV of datasource column definitions, to be added to a config. '
                             'Use with generate_config or merge_config')
group_file.add_argument('--include_extract', action='store_true',
                        help='Includes the extract in the download if specified. '
                             'This will make downloads take a long time for large extracts.')
group_file.add_argument('-tds', '--save_tds', action='store_true',
                        help='Saves the TDS for the datasource to view the raw xml')

# GROUP: CONNECTION
group_connection = parser.add_argument_group(
    'connection', 'Args which provide information for a Tableau Datasource connection.'
)
group_connection.add_argument('--conn_user', help='Connection Username')
group_connection.add_argument('--conn_pw', help='Connection Password')
group_connection.add_argument('--conn_type', help='Connection type; e.g. snowflake')
group_connection.add_argument('--conn_db', help='Connection Database')
group_connection.add_argument('--conn_schema', help='Connection Schema')
group_connection.add_argument('--conn_host', help='Connection Host (URL)')
group_connection.add_argument('--conn_role', help='Connection Role')
group_connection.add_argument('--conn_warehouse', help='Connection Warehouse')

# SERVER INFO
parser_server_info = subparsers.add_parser(
    'server_info',
    help='Retrieve and view information from Tableau Cloud/Server'
)
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
parser_server_operate = subparsers.add_parser(
    'server_operate', help='Download, publish, and refresh objects on Tableau Cloud/Server'
)
parser_server_operate.add_argument('--embed_connection', action='store_true',
                                   help='Specify to embed credentials for a Tableau Datasource Connection.'
                                        ' --conn_user and --conn_pw are required')
parser_server_operate.add_argument('--download', choices=['datasource', 'workbook'],
                                   help='Specify to download a Tableau object')
parser_server_operate.add_argument(
    '--publish', choices=['datasource', 'workbook'],
    help='Specify to publish a Tableau object. '
         '(Optional) Include --conn_user and --conn_pw to embed creds for the connection.'
)
parser_server_operate.add_argument('--refresh', choices=['datasource', 'workbook'],
                                   help='Specify to refresh a Tableau object')
parser_server_operate.add_argument('--all',  action='store_true', help='Download all workbooks or datasources')
parser_server_operate.set_defaults(func=server_operate)

# DATASOURCE
parser_datasource = subparsers.add_parser('datasource', help='View and edit metadata about the datasource')
parser_datasource.add_argument(
    '--enforce_connection', action='store_true',
    help='Provide to enforce the Datasource Connection attributes; See connection CLI group.'
)
parser_datasource.add_argument('--delete', choices=['folder', 'column'],
                               help='Deletes the specified object. The name of the object must be specified; '
                                    '--folder_name --column_name')
parser_datasource.add_argument('--list', choices=['folders', 'columns', 'metadata', 'connections'],
                               help='Lists the specified objects.')
parser_datasource.add_argument('--folder_name', help='The name of the folder. Required for --delete folder')
parser_datasource.add_argument('--column_name', help='The local name of the column. Required.')
parser_datasource.add_argument('--remote_name', help='The remote (SQL) name of the column.')
parser_datasource.add_argument('--caption', help='Short name/Alias for the column')
parser_datasource.add_argument('--title_case_caption', action='store_true',
                               help='Converts caption to title case. Applied after --caption')
parser_datasource.add_argument('--persona', choices=list(personas.keys()),
                               help='The datatype persona of the column. Required for adding a new column')
parser_datasource.add_argument('--desc', help='A Tableau column description')
parser_datasource.add_argument('--calculation', help='A Tableau calculation')
parser_datasource.set_defaults(func=datasource)

# GENERATE CONFIG
parser_config_gen = subparsers.add_parser(
    'generate_config', help='Generate configs to programmatically manage metadata in Tableau '
                            'datasources via Airflow.\nRequires File arguments.\n')
parser_config_gen.add_argument('--file_prefix', action='store_true',
                               help='Adds a prefix of the --name File arg to the output configs names.')
parser_config_gen.set_defaults(func=generate_config)

# CONFIG TO CSV
parser_config_csv = subparsers.add_parser(
    'csv_config', help='Write a config to a csv with 1 row per column per datasource')
parser_config_csv.add_argument('-cl', '--config_list', nargs='+', help='The list of paths to the configs')
parser_config_csv.set_defaults(func=csv_config)


# MERGE CONFIG
parser_config_merge = subparsers.add_parser(
    'merge_config', help='Merge a new config into the existing master config', formatter_class=RawTextHelpFormatter)
parser_config_merge.add_argument(
    '-mw', '--merge_with', choices=['config', 'csv', 'generate_merge_all'],
    help='config: merge an existing config with a new config\n'
         'csv: merge an existing config with a csv containing datasource column definitions\n'
         'generate_merge_all: Runs generate_config to create a column_config and a calc_config from a datasource. '
         'Merges both with existing configs in a target directory')
parser_config_merge.add_argument('-e', '--existing_config', help='The path to the base configuration file.')
parser_config_merge.add_argument('-a', '--additional_config',
                                 help='The path to the configuration to add to --existing_config.')
parser_config_merge.add_argument('-m', '--merged_config', default='merged_config',
                                 help='Name of the file to write the merged configuration file to. '
                                      'i.e. "my_config" will output to "my_config.json".')
parser_config_merge.add_argument('-td', '--target_directory', default='merged_config',
                                 help='The path containing the existing configs. '
                                      'Use with --merge_with generate_merge_all')
parser_config_merge.set_defaults(func=merge_configs)


def validate_args_server_operate(args):
    """ Validate that combinations of args are present """
    if (args.name and not args.project_name) or (args.project_name and not args.name):
        parser.error('--name and --project_name are required together')

    if args.publish and (args.name is None or args.project_name is None or args.file_path is None):
        parser.error('--publish requires: --name --project_name and --file_path ')

    if not (args.download or args.publish or args.refresh or args.embed_connection):
        parser.error('server_operate must be called with one of: '
                     '--download --publish --refresh --embed_connection')

    if len([i for i in [args.download, args.publish, args.refresh, args.embed_connection] if i]) > 1:
        parser.error('server_operate cannot be called with more than one of: '
                     '--download --publish --refresh --embed_connection')

    if args.embed_connection and (args.conn_user is None or args.conn_pw is None):
        parser.error('Both --conn_user and --conn_pw must be provided with --embed_connection')


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


def validate_args_command_datasource(args):
    """ Validates args for the datasource command """
    if args.delete == 'folder' and not args.folder_name:
        parser.error(f'{args.command} --delete folder requires --folder_name')

    if args.delete == 'column' and not args.column_name:
        parser.error(f'{args.command} --delete column requires --column_name')


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
    """ Creates the Tableau server authentication from a variety of methods for passing in credentials """
    debug = args.debugging_logs
    yaml_path = args.settings_path
    color = Color()
    symbol = Symbol()

    if debug:
        title = f'{symbol.line * 29} Tableau Server Authentication {symbol.line * 29}'
        sub_title = ' Credentials are prioritized by: CLI Arguments > Settings YAML > Environment Variables '
        title_color = {'fg': 'green'}
        color_print(title, **title_color)
        print(f'{color.fg_black}{color.bg_yellow}{sub_title}{color.reset}')

    # Set CLI Argument credentials
    creds = {
        'user': args.user,
        'password': args.password,
        'api_version': args.api_version,
        'token_name': args.token_name,
        'token_secret': args.token_secret,
        'site': args.site_name,
        'server': args.server
    }

    if debug:
        for cred_name, cred_value in creds.items():
            if cred_value:
                print(f'  {symbol.arrow_r} Using CLI Argument cred: '
                      f'{cred_name} = {color.fg_cyan}{cred_value}{color.reset}')

    # Set Settings YAML file credentials
    if yaml_path and os.path.exists(yaml_path):
        with open(yaml_path, 'r') as f:
            yaml_creds = yaml.safe_load(f).get('tableau_login', {})
        if debug and not yaml_creds:
            color_print('YAML detected, but "tableau_login" is not defined', fg='yellow')
        for cred_name, cred_value in yaml_creds.items():
            if cred_value and cred_name in creds and not creds[cred_name]:
                creds[cred_name] = cred_value
                if debug:
                    print(f'  {symbol.arrow_r} Using Settings YAML cred: '
                          f'{cred_name} = {color.fg_cyan}{cred_value}{color.reset}')

    # Set Environment Variables credentials
    env_creds = {
        'token_name': os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_NAME"),
        'token_secret': os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_VALUE"),
        'site': os.getenv("TABLEAU_SITENAME"),
        'server': os.getenv("TABLEAU_SERVER_ADDRESS")
    }
    for cred_name, cred_value in env_creds.items():
        if cred_value and not creds[cred_name]:
            creds[cred_name] = cred_value
            if debug:
                print(f'  {symbol.arrow_r} Using Environment Variable cred: '
                      f'{cred_name} = {color.fg_cyan}{cred_value}{color.reset}')

    # Validate the combinations for authentication methods
    # If one, but not both, of token_name/token_secret or username/password are provided, throw an error
    if (creds['token_secret'] or creds['token_name']) and not (creds['token_name'] and creds['token_secret']):
        parser.error('--token_secret and --token_name are required together')
    if (creds['user'] or creds['password']) and not (creds['password'] and creds['user']):
        parser.error('--password and --user are required together')

    if debug:
        # Prints ending lines based on the title and title color printed above
        color_print(symbol.line * len(title), **title_color)
        print()  # new line

    # Create the server object and run the functions
    t = ts.TableauServer(
        personal_access_token_name=creds['token_name'],
        personal_access_token_secret=creds['token_secret'],
        user=creds['user'],
        password=creds['password'],
        site=creds['site'],
        host=f'https://{creds["server"]}.online.tableau.com',
        api_version=creds['api_version']
    )
    if debug:
        color_print(symbol.success, ' Connected to Tableau Server', **title_color)

    return t


def set_datasource_connection_args(args):
    """ Updates the connection group args, using a settings YAML file, and/or Environment Variables """
    debug = args.debugging_logs
    yaml_path = args.settings_path
    color = Color()
    symbol = Symbol()

    if debug:
        title = f'{symbol.line * 15} Setting Connection Group Arguments {symbol.line * 15}'
        sub_title = ' Credentials are prioritized by: CLI Arguments > Settings YAML > Environment Variables '
        title_color = {'fg': 'green'}
        color_print(title, **title_color)
        print(f'{color.fg_black}{color.bg_yellow}{sub_title}{color.reset}')

    # Set CLI Argument credentials
    creds = {
        'conn_user': args.conn_user,
        'conn_pw': args.conn_pw,
        'conn_type': args.conn_type,
        'conn_db': args.conn_db,
        'conn_schema': args.conn_schema,
        'conn_host': args.conn_host,
        'conn_role': args.conn_role,
        'conn_warehouse': args.conn_warehouse
    }
    if debug:
        for cred_name, cred_value in creds.items():
            if cred_value:
                print(f'  {symbol.arrow_r} Using CLI Argument cred: '
                      f'{cred_name} = {color.fg_cyan}{cred_value}{color.reset}')

    # Set Settings YAML file credentials
    if yaml_path and os.path.exists(yaml_path):
        with open(yaml_path, 'r') as f:
            yaml_creds = yaml.safe_load(f).get('datasource_connection', {})
        if debug and not yaml_creds:
            color_print('YAML detected, but "datasource_connection" is not defined', fg='yellow')
        for cred_name, cred_value in yaml_creds.items():
            if cred_value and cred_name in creds and not creds[cred_name]:
                creds[cred_name] = cred_value
                if debug:
                    print(f'  {symbol.arrow_r} Using Settings YAML cred: '
                          f'{cred_name} = {color.fg_cyan}{cred_value}{color.reset}')

    # Set Environment Variables credentials
    env_creds = {
        'conn_type': os.getenv('CONN_TYPE'),
        'conn_host': os.getenv('CONN_HOST'),
        'conn_user': os.getenv('CONN_USERNAME'),
        'conn_pw': os.getenv('CONN_PASSWORD'),
        'conn_db': os.getenv('CONN_DB'),
        'conn_schema': os.getenv('CONN_SCHEMA'),
        'conn_role': os.getenv('CONN_ROLE'),
        'conn_warehouse': os.getenv('CONN_WAREHOUSE')
    }
    for cred_name, cred_value in env_creds.items():
        if cred_value and not creds[cred_name]:
            creds[cred_name] = cred_value
            if debug:
                print(f'  {symbol.arrow_r} Using Environment Variable cred: '
                      f'{cred_name} = {color.fg_cyan}{cred_value}{color.reset}')

    if debug:
        # Prints ending lines based on the title and title color printed above
        color_print(symbol.line * len(title), **title_color)
        print()  # new line

    # Update Datasource Connection Group Args
    for arg, value in creds.items():
        setattr(args, arg, value)

    if debug:
        color_print(symbol.success, ' Set Datasource Connection Group Args', **title_color)


def main():
    args = parser.parse_args()

    # Set absolute path of the settings_path, if it exists and is not already absolute
    if not os.path.isabs(args.settings_path) and os.path.exists(args.settings_path):
        args.settings_path = os.path.abspath(args.settings_path)

    # Set args from connection group, from settings YAML / Environment Variables, when not provided in the command
    set_datasource_connection_args(args)

    # Validate the arguments
    if args.location == 'local' and args.file_path is None:
        parser.error('--location local requires --file_path')
    if args.command == 'server_operate':
        validate_args_server_operate(args)
    if args.command in ['generate_config', 'datasource']:
        validate_args_id_name_project(args)
    if args.command == 'datasource':
        validate_args_command_datasource(args)
    if args.command == 'merge_config':
        validate_args_command_merge_config(args)

    # Set/Reset the directory
    tmp_folder = args.output_dir
    if args.clean_dir:
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
        server = tableau_authentication(args)
        args.func(args, server)
    # Run functions that don't need the server
    else:
        args.func(args)


if __name__ == '__main__':
    main()
