import os
from pprint import pprint

import yaml

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def connection_settings(args, debugging_logs, settings_path=None):
    """ Creates a dictionary with named connection information

    Args:
        args: An argparse args object
        debugging_logs (bool): True to print logging messages
        settings_path (str): The path to a local settings YAML file

    Returns: Dictionary with the connection information
    """
    if debugging_logs:
        print('Building Connection Credentials')
        print('Connection creds are prioritized by: CLI Arguments > Settings YAML > Environment Variables')

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
    if debugging_logs:
        for name, value in creds.items():
            if value:
                print(f'Using CLI Argument cred: {name} = {value}')

    # Set Settings YAML credentials
    if settings_path:
        with open(settings_path, 'r') as f:
            yaml_creds = yaml.safe_load(f)
            yaml_creds = yaml_creds['embed_connection']
        for name, value in yaml_creds.items():
            if name in creds and not creds[name] and value:
                creds[name] = value
                if debugging_logs:
                    print(f'Using Settings YAML cred: {name} = {value}')

    # Set Environment Variables credentials
    env_settings = {
        'conn_user': os.getenv("CONN_USERNAME"),
        'conn_pw': os.getenv("CONN_PASSWORD"),
        'conn_type': os.getenv("CONN_TYPE"),
        'conn_db': os.getenv("CONN_DB"),
        'conn_schema': os.getenv("CONN_SCHEMA"),
        'conn_host': os.getenv("CONN_HOST"),
        'conn_role': os.getenv("CONN_ROLE"),
        'conn_warehouse': os.getenv("CONN_WAREHOUSE")
    }
    for name, value in env_settings.items():
        if not creds[name] and value:
            creds[name] = value
            if debugging_logs:
                print(f'Using Environment Variable cred: {name} = {value}')

    if debugging_logs:
        for name, value in creds.items():
            if not value:
                print(f'Missing cred: {name}')

    return creds


def update_connection(datasource, conn_settings, debugging_logs):
    """ Updates the datasource connection object with the conn_settings provided.

    Args:
        datasource (Datasource): The datasource object
        conn_settings (dict): The connection settings dict
        debugging_logs (bool): True to print logging messages
    """

    if debugging_logs:
        print('Updating the datasource connection')

    connection_type = conn_settings['conn_type']

    datasource.connection.named_connections[connection_type].connection.server = conn_settings['conn_host']
    datasource.connection.named_connections[connection_type].connection.username = conn_settings['conn_user']
    datasource.connection.named_connections[connection_type].connection.service = conn_settings['conn_role']
    datasource.connection.named_connections[connection_type].connection.dbname = conn_settings['conn_db']
    datasource.connection.named_connections[connection_type].connection.schema = conn_settings['conn_schema']
    datasource.connection.named_connections[connection_type].connection.warehouse = conn_settings['conn_warehouse']

    datasource.save()
    if debugging_logs:
        print('Datasource file saved')


def embed_credential_online(datasource_id, conn_settings, server, debugging_logs=False):
    """ Embeds a username and password in a datasource online

    Args:
        datasource_id: The ID of the datasource to update credentials for
        conn_settings: The connection settings dictionary
        server: The TableauServer connection class
        debugging_logs (bool): True to print logging messages
    """

    if debugging_logs:
        print(f'UPDATING CREDENTIALS FOR {datasource_id}')
        print(f"CONNECTION USER: {conn_settings['conn_user']}")
        print(f"CONNECTION PASSWORD: {conn_settings['conn_pw']}")

    response = server.embed_datasource_credentials(
            datasource_id,
            credentials={
                'username': conn_settings['conn_user'],
                'password': conn_settings['conn_pw']
            },
            connection_type=conn_settings['conn_type']
        )

    if debugging_logs:
        print(f'SERVER RESPONSE {response}')


def connection(args, server=None):
    """ Updates a Datasource's connection in the file locally

    Args:
        args: An argparse args object
        server (TableauServer): The TableauServer object
    """

    # Set Args
    datasource_id = args.id
    debugging_logs = args.debugging_logs
    datasource_path = args.file_path
    settings_yaml = args.settings_path
    connection_operation = args.connection_operation

    # Set the connection settings dictionary
    conn_settings = connection_settings(args, debugging_logs, settings_yaml)

    if connection_operation == 'update_local_connection':
        print('Local Datasource, Updating Connection')
        datasource = Datasource(datasource_path)
        update_connection(datasource, conn_settings, debugging_logs)

    elif connection_operation == 'embed_user_pass':
        print('Online Datasource, Updating Embedded Username and Password')

        if debugging_logs:
            print(f'EMBEDDING CREDS FOR DATASOURCE ID: {datasource_id}')

        if datasource_id is None:
            datasource_id = server.get_datasource(datasource_name=args.name, datasource_project=args.project_name).id

        embed_credential_online(datasource_id, conn_settings, server, debugging_logs)



