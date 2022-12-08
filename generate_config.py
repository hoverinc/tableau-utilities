import json
import argparse
import os
import yaml
import shutil
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
    return parser.parse_args()


def generate_config(server, datasource_name):
    """ Gets a list of all columns in all datasources

    Args:
        server (TableauServer): A Tableau server object
    """
    shutil.rmtree('tmp_tdsx', ignore_errors=True)
    tmp_folder = 'tmp_tdsx'
    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)
    datasource_list = [d for d in server.get_datasources()]
    rows = dict()

    for datasource in datasource_list:

        if datasource.name == datasource_name:
            print("BUILDING CONFIG FOR:", datasource.project_name, (datasource.id, datasource.name))
        else:
            print("SKIPPING:", datasource.project_name, (datasource.id, datasource.name))




    #     datasource_path = server.download_datasource(datasource.id, include_extract=False)
    #     columns = [c.dict() for c in Datasource(datasource_path).columns]
    #     rows.setdefault(datasource.name, [])
    #     rows[datasource.name].extend(columns)
    # os.chdir('')
    # shutil.rmtree(tmp_folder)
    return rows


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
