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
    parser.add_argument('--list_datasources', action='store_true',
                        help='Print a list of datasources to the console')
    parser.add_argument('--show_datasources', action='store_true',
                        help='Print a table with datasource info to the console')
    return parser.parse_args()


def datasource_info(server, list_datasources=False, show_datasources=False, sort_field='name'):
    """ Downloads the specified datasources


    Args:
        server (TableauServer): A Tableau server object
        datasource_name: The name of the datasource to download
        list_datasources: Prints a sorted list of all the datasources from a site

    Returns:
        datasource: The datasource object for the datasource that was downloaded
        datasource_path: The path of the datasource that was downloaded
    """
    datasource_list = [d for d in server.get_datasources()]

    sources = []
    for datasource in datasource_list:
        if list_datasources:
            print(datasource.__dict__)
            info = datasource.__dict__
            sources.append(info)

    sorted_sources = sorted(sources, key=lambda d: d[sort_field])

    if list_datasources:
        for source in sorted_sources:
            print(source['name'])



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
        datasource_info(ts, list_datasources=True)

