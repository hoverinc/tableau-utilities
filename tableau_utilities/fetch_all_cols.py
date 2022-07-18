import json
import argparse
import os
import yaml
import shutil
from tableau_utilities import Datasource, TableauServer


def do_args():
    """ Parse arguments.

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(description='List columns in Tableau datasources')
    parser.add_argument(
        '--server',
        help='Tableau Server URL. i.e. <server_address> in https://<server_address>.online.tableau.com',
        default=None
    )
    parser.add_argument(
        '--site',
        help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
        default=None)
    parser.add_argument('--api_version', help='Tableau API version', default='2.8')
    parser.add_argument('--user', required=True, help='user name')
    parser.add_argument('--password', required=True, help='password')
    return parser.parse_args()


def all_columns_all_datasources(server):
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
        print(datasource.project_name, (datasource.id, datasource.name))
        datasource_path = server.download_datasource(datasource.id, include_extract=False)
        columns = [c.dict() for c in Datasource(datasource_path).columns]
        rows.setdefault(datasource.name, [])
        rows[datasource.name].extend(columns)
    os.chdir('..')
    shutil.rmtree(tmp_folder)
    return rows


if __name__ == '__main__':
    args = do_args()
    with open('settings.yaml') as f:
        settings = yaml.safe_load(f)
    if args.server:
        host = f'https://{args.server}.online.tableau.com'
    else:
        host = settings['tableau_login']['host']
    ts = TableauServer(
        user=args.user or settings['tableau_login']['user'],
        password=args.password or settings['tableau_login']['password'],
        site=args.site or settings['tableau_login']['site'],
        host=host,
        api_version=args.api_version or settings['tableau_login']['api_version']
    )
    config = all_columns_all_datasources(ts)
    with open('generated_config.json', 'w') as fd:
        json.dump(config, fd, indent=3)
