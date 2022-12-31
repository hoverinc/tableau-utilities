import pandas as pd
from tabulate import tabulate
from pprint import pprint

from tableau_utilities.tableau_server.tableau_server import TableauServer


def object_list_to_dicts(object_list):
    """ Turns a list of objects from the Tableau client to a dictionary

    Args:
        object_list: A list of objects from the Tableau client

    Returns:
        A dictionary version of the object_list

    """

    records = []
    for object in object_list:
        info = object.__dict__
        records.append(info)

    return records


def print_info(object_list, format, sort_field='name'):
    """ Prints information to console for a list of objects

    Args:
        object_list: The response from Tableau Servers with the list of objects
        format: The format for printing the information
        sort_field: The field to sort the result on


        server (TableauServer): A Tableau server object
        datasource_name: The name of the datasource to download
        list_datasources: Prints a sorted list of all the datasources from a site

    Returns:
        None
    """

    records = object_list_to_dicts(object_list)

    sorted_records = sorted(records, key=lambda d: d[sort_field])

    if format == 'names':
        for record in sorted_records:
            print(record['name'])
    elif format == 'names_ids':
        for record in sorted_records:
            print(record['name'], record['id'])
    elif format == 'ids_names':
        for record in sorted_records:
            print(record['id'], record['name'])
    elif format == 'full_df':
        df = pd.DataFrame(sorted_records)
        # df = df[['name', 'id']]
        print(tabulate(df, headers='keys', tablefmt='psql', colalign='left'))
    elif format == 'full_dictionary':
        for record in sorted_records:
            print(record)
    elif format == 'full_dictionary_pretty':
        for record in sorted_records:
            pprint(record)


def server_info(args, server):
    """ Prints information for datasources, projects, or workbooks

    Args:
        args: The args from the cli
        server: the Tableau Server authentication object

    """
    if args.list_object == 'datasource':
        object_list = [d for d in server.get_datasources()]
    if args.list_object == 'project':
        object_list = [p for p in server.get_projects()]
    if args.list_object == 'workbook':
        object_list = [w for w in server.get_workbooks()]

    print_info(object_list, args.list_format, args.list_sort_field)

