from argparse import RawTextHelpFormatter
import argparse
import os
import shutil
import yaml

from tableau_utilities.tableau_server.tableau_server import TableauServer
from tableau_utilities.scripts.gen_config import generate_config
from tableau_utilities.scripts.merge_config import merge_configs
from tableau_utilities.scripts.server_info import server_info
from tableau_utilities.scripts.server_operate import server_operate


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
    '--server',
    help='Tableau Server URL. i.e. <server_address> in https://<server_address>.online.tableau.com',
    default=None
)
group_server.add_argument(
    '--site',
    help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
    default=None)
group_server.add_argument('--api_version', help='Tableau API version', default='3.17')

# GROUP: USER & PASSWORD
group_user_password = parser.add_argument_group('user_pass', 'Authentication with username and password method')
group_user_password.add_argument('--user', help='user name')
group_user_password.add_argument('--password', help='password')

# GROUP: PERSONAL ACCESS TOKENS
group_token = parser.add_argument_group('token_info', 'Authentication with a Personal Access Token (PAT)')
group_token.add_argument('--token_secret', help='Personal Access Token Secret')
group_token.add_argument('--token_name', help='Personal Access Token Name')

# GROUP: SETTINGS YAML
group_settings_yaml = parser.add_argument_group('settings.yaml', 'Authentication with settings in a .yaml file' )
group_settings_yaml.add_argument('--settings_path',
                                 help='Path to your local settings.yaml file (See sample_settings.yaml)'
)

# GROUP LOCAL FOLDER
group_local_folder = parser.add_argument_group('local_folder', 'Manage where to read/write files')
group_local_folder.add_argument('--folder_name', default='tmp_tdsx_and_config',
                                help='Specifies the folder to write the datasource and configs to')
group_local_folder.add_argument('--clean_up_first', action='store_true',
                                help='Deletes the directory and files before running')

# SERVER INFO
parser_server_info = subparsers.add_parser('server_info',
                                           help='Retrieve and view information from Tableau Cloud/Server')
parser_server_info.add_argument('--list_object', choices=['datasource', 'project', 'workbook'], required=True,
                                help='Specify the type of object for the information.')
parser_server_info.add_argument('--list_format', default='names',
                                choices=['names', 'names_ids', 'ids_names', 'full_df', 'full_dictionary',
                                         'full_dictionary_pretty'],
                                help='Set the fields and format for the information.')
parser_server_info.add_argument('--list_sort_field', default='name',
                                help='Choose the field for sorting the information.')
parser_server_info.set_defaults(func=server_info)

# DOWNLOAD & PUBLISH
parser_server_operate = subparsers.add_parser('server_operate',
                                               help='Download, publish, and refresh objects on Tableau Cloud/Server')
parser_server_operate.add_argument('--action_type', choices=['download', 'publish', 'refresh'], required=True,
                                    help='List information about the Object')
parser_server_operate.add_argument('--object_type', choices=['datasource', 'workbook'],  required=True,
                                    help='List information about the Object')
parser_server_operate.add_argument('--id', help='The ID for the object on Tableau Cloud/Server')
parser_server_operate.add_argument('--name',  help='The datasource or workbook name. User with --project_name.')
parser_server_operate.add_argument('--project_name', help='The project name for the datasource or workbook. Use with --name.')
parser_server_operate.add_argument('--file_path', help='The path to the file to publish')
parser_server_operate.add_argument('--include_extract', action='store_true',
                                    help='Includes the extract in the download if specified. '
                                         'This will make downloads take a long time for large extracts.')
parser_server_operate.set_defaults(func=server_operate)

# GENERATE CONFIG
parser_config_gen = subparsers.add_parser('generate_config',
                                          help='Generate configs to programatically manage metdatadata in Tableau '
                                               'datasources via Airflow.')
parser_config_gen.add_argument('--datasource_source', choices=['local', 'online'],  required=True,
                                   help='Specify the location of the datasource.')
parser_config_gen.add_argument('-dsp', '--datasource_path', help='The name of the datasource to generate a config for.')
parser_config_gen.add_argument('-dsid', '--datasource_id', help='The name of the datasource to generate a config for.')
parser_config_gen.add_argument('-dsn', '--datasource_name', help='The name of the datasource to generate a config for.')
parser_config_gen.add_argument('-dspn', '--datasource_project_name', help='The name of project that has the datasource')
parser_config_gen.add_argument('--file_prefix', action='store_true',
                               help='Adds a prefix of the datasource name to the output file names.')
parser_config_gen.add_argument('--definitions_csv',
                               help='Add data defintions from a csv to the config. It may be easier to bulk '
                                    'populate definitions in a spreadsheet than in the config.')
parser_config_gen.set_defaults(func=generate_config)

# MERGE CONFIG
parser_config_merge = subparsers.add_parser('merge_config',
                                            help='Merge a new config into the existing master config')
parser_config_merge.add_argument('--existing_config', required=True,
                                 help='The path to the current configuration. The current configuration may have '
                                      'more than 1 datasource.')
parser_config_merge.add_argument('--additional_config',  required=True,
                                 help='The path to the configuration to add. This code ASSUMES that the additional '
                                      'config is for a single datasource ')
parser_config_merge.add_argument('--merged_config', default='merged_config',
                                 help='The name of the merged config JSON file. Ex: filename "my_config.json" '
                                      'argument is "my_config" Do not enter the .json extension.')
parser_config_merge.set_defaults(func=merge_configs)


def validate_args_server_operate(args):
    """ Validate that combinations of args are present
    """
    if (args.name and not args.project_name) or (args.project_name and not args.name):
        parser.error('--name and --project_name are required together')


def validate_args_generate_config(args):
    """ Validate that combinations of args are present

    """

    if args.datasource_source == 'local' and not args.datasource_path:
        parser.error('--datasource_source local requires a --datasource_path')

    if args.datasource_source == 'online':
        if args.datasource_id:
            pass
        elif (args.datasource_name and not args.datasource_project_name) or (args.datasource_project_name and not args.datasource_name):
            parser.error('--datasource_name and --datasource_project_name are required together')
        elif not args.datasource_id and not args.datasource_name and not args.datasource_project_name:
            parser.error('--datasource_source online requires either a --datasource_id or a --datasource_name and --datasource_project_name')


def validate_auth_included(args):
    """ Validates that auth is included for operations that need it and that the parameters are present

    """

    if not args.auth:
        parser.error('These commands required --auth method and credentials to authenticate with Tableau Server/Online')
    if args.auth == 'args_user_pass':
        if not args.user or  not args.password:
            parser.error('You must include --user and --password for args_user_pass authentication')
    if args.auth == 'args_token':
        if not args.token_name or not args.token_secret:
            parser.error('You must include --token_name and --token_secret for args_token authentication')
    if args.auth == 'settings_yaml' and not args.settings_path:
        parser.error('You must include --settings_path for settings_yaml authentication')


def tableau_authentication(args):
    """ Creates the Tableau server authentication from a variety of methods for passing in credentials

    """

    # Set the defaults from the args
    user = args.user
    password = args.password
    token_name = args.token_name
    token_secret = args.token_secret
    site = args.site
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
    elif args.auth =='os_env':
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
    if args.command == 'generate_config':
        validate_args_generate_config(args)

    # Set/Reset the directory
    tmp_folder = args.folder_name
    if args.clean_up_first:
        shutil.rmtree(tmp_folder, ignore_errors=True)

    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)

    needs_tableau_server = (
        (args.command == 'generate_config' and args.datasource_source == 'online')
        or args.command == 'server_info'
        or args.command == 'server_operate'
    )

    if needs_tableau_server:
        validate_auth_included(args)
        ts = tableau_authentication(args)
        args.func(args, ts)

    # Run functions that don't need the server
    else:
        args.func(args)


if __name__ == '__main__':
    main()



