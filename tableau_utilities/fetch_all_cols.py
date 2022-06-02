import json
import argparse
import os
import shutil
from tableau_utilities import TDS, TableauServer, extract_tds


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
        server (obj): A Tableau server object
    """
    shutil.rmtree('tmp_tdsx', ignore_errors=True)
    datasource_list = server.list_datasources(server, print_it=False)
    rows = []
    for project_and_dsname in datasource_list:
        print(project_and_dsname, datasource_list[project_and_dsname])
        tdsx = server.download_datasource(datasource_list[project_and_dsname], include_extract=False)
        os.mkdir('tmp_tdsx')
        shutil.move(tdsx, 'tmp_tdsx')
        os.chdir('tmp_tdsx')
        tds_dict = extract_tds(os.path.basename(tdsx))
        columns = TDS(tds_dict).list('column')
        rows.extend(columns)
    return rows


args = do_args()
ts = TableauServer(
    user=args.user,
    password=args.password,
    site=args.site,
    url=f'https://{args.server}.online.tableau.com',
    api_version=args.api_version
)
config = all_columns_all_datasources(ts)
with open('generated_config.json', 'w') as fd:
    json.dump(config, fd, indent=2)
