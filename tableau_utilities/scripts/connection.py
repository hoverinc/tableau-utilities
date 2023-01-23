import os
import sys
from pprint import pprint


import tableau_utilities as tu
from tableau_utilities.tableau_server.tableau_server import TableauServer
from tableau_utilities.scripts.server_operate import get_object_list, fill_in_id_name_project


def connection_settings(args, debugging_logs, settings):
    """ Creates a dictionary with named connection information

    Returns:
        connection_settings: Dictionary with the connection information
    """

    conn = args.conn

    if debugging_logs:
        print('Building the connection settings')

    conn_user = args.conn_user
    conn_pw = args.conn_pw
    conn_type = args.conn_type
    conn_db = args.conn_db
    conn_schema = args.conn_schema
    conn_host = args.conn_host
    conn_role = args.conn_role
    conn_warehouse = args.conn_warehouse

    if conn == 'settings_yaml':
        if debugging_logs:
            print("Building connection settings from yaml file")
        conn_user = settings['embed_connection']['username']
        conn_pw = settings['embed_connection']['password']
        conn_type = settings['embed_connection']['class_name']
        conn_db = settings['embed_connection']['dbname']
        conn_schema = settings['embed_connection']['schema']
        conn_host = settings['embed_connection']['server']
        conn_role = settings['embed_connection']['service']
        conn_warehouse = settings['embed_connection']['warehouse']
    elif conn == 'os_env':
        if debugging_logs:
            print("Building connection settings from environment variables")
        conn_user = os.getenv("CONN_USERNAME")
        conn_pw = os.getenv("CONN_PASSWORD")
        conn_type = os.getenv("CONN_TYPE")
        conn_db = os.getenv("CONN_DB")
        conn_schema = os.getenv("CONN_SCHEMA")
        conn_host = os.getenv("CONN_HOST")
        conn_role = os.getenv("CONN_ROLE")
        conn_warehouse = os.getenv("CONN_WAREHOUSE")


    connection_settings = {'conn_user': conn_user,
                           'conn_pw': conn_pw,
                           'conn_type': conn_type,
                           'conn_db': conn_db,
                           'conn_schema': conn_schema,
                           'conn_host': conn_host,
                           'conn_role': conn_role,
                           'conn_warehouse': conn_warehouse
                           }

    if debugging_logs:
        print("Connection settings")
        pprint(connection_settings)


    return connection_settings


def update_connection(datasource, conn_settings, debugging_logs):
    """

    Args:
        datasource: The datasource object
        conn_settings: The connection setttings dictionary

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
    """

    """

    # Set Args
    datasource_id = args.id
    name = args.name
    project_name = args.project_name
    debugging_logs = args.debugging_logs
    datasource_path = args.file_path
    settings_yaml = args.settings_path

    # Set the connection settings dictionary
    conn_settings = connection_settings(args, debugging_logs, settings_yaml)

    if args.connection_operation == 'update_local_connection':
        print('Local Datasource, Updating Connection')
        datasource = tu.Datasource(datasource_path)
        update_connection(datasource, conn_settings)

    elif args.connection_operation == 'embed_user_pass':
        print('Online Datasource, Updating Embedded Username and Password')

        if datasource_id is None:
            # Get the datasource ID
            object_list = get_object_list(object_type='datasource', server=server)
            datasource_id, name, project_name = fill_in_id_name_project(datasource_id, name,
                                                                               project_name, object_list)
            if debugging_logs:
                print(
                    f'GOT DATASOURCE ID: {datasource_id}, NAME: {name}, PROJECT NAME: {project_name}')

            embed_credential_online(datasource_id, conn_settings, server, debugging_logs)




