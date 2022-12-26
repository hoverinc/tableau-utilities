import argparse
import pandas as pd
from tabulate import tabulate
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def print_info(object_list, verbosity, sort_field='name'):
    """ Downloads the specified datasources
    Args:
        server (TableauServer): A Tableau server object
        datasource_name: The name of the datasource to download
        list_datasources: Prints a sorted list of all the datasources from a site
    Returns:
        datasource: The datasource object for the datasource that was downloaded
        datasource_path: The path of the datasource that was downloaded
    """

    records = []
    for object in object_list:
        info = object.__dict__
        records.append(info)

    sorted_records = sorted(records, key=lambda d: d[sort_field])

    if verbosity == 'names':
        for record in sorted_records:
            print(record['name'])
    elif verbosity == 'names_ids':
        for record in sorted_records:
            print(record['name'], record['id'])
    elif verbosity == 'ids_names':
        for record in sorted_records:
            print(record['id'], record['name'])
    elif verbosity == 'full_df':
        df = pd.DataFrame(sorted_records)
        # df = df[['name', 'id']]
        print(tabulate(df, headers='keys', tablefmt='psql', colalign='left'))


def server_info(args, server):
    if args.list_object == 'datasource':
        object_list = [d for d in server.get_datasources()]
        # datasource_info(server, args.list_verbosity, args.list_sort_field)
    if args.list_object == 'project':
        object_list = [p for p in server.get_projects()]
        # datasource_info(server, args.list_verbosity, args.list_sort_field)
    if args.list_object == 'workbook':
        object_list = [w for w in server.get_workbooks()]
        # datasource_info(server, args.list_verbosity, args.list_sort_field)

    print_info(object_list, args.list_verbosity, args.list_sort_field)

