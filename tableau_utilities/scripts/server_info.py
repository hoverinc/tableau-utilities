import argparse
import pandas as pd
from tabulate import tabulate
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def datasource_info(server, verbosity, sort_field='name'):
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
        info = datasource.__dict__
        sources.append(info)

    sorted_sources = sorted(sources, key=lambda d: d[sort_field])

        # if list_datasources:
        #     print(datasource.__dict__)
        #     info = datasource.__dict__
        #     sources.append(info)


    if verbosity == 'names':
        for source in sorted_sources:
            print(source['name'])
    elif verbosity == 'names_ids':
        for source in sorted_sources:
            print(source['name'], source['id'])

    # # TO DO: This is only printing a pretty table when columns are limited
    # if show_datasources:
    #     df = pd.DataFrame(sources)
    #     df = df[['name', 'created_at', 'is_certified']]
    #     # print(df.to_markdown())
    #     print(tabulate(df, headers='keys', tablefmt='psql', colalign='left'))


def server_info(args, server):
    if args.list_object == 'datasource':
        datasource_info(server, args.list_verbosity, args.list_sort_field)
    if args.list_object == 'project':
        datasource_info(server, args.list_verbosity, args.list_sort_field)
    if args.list_object == 'workbook':
        datasource_info(server, args.list_verbosity, args.list_sort_field)

