import pandas as pd
from tabulate import tabulate
from pprint import pprint

from tableau_utilities.tableau_server.tableau_server import TableauServer


def server_info(args, server):
    """ Prints information for datasources, projects, or workbooks

    Args:
        args: The args from the CLI
        server (TableauServer): the Tableau Server authentication object

    """
    format = args.list_format
    sort_field = args.list_sort_field

    if args.list_object:
        # Get server objects, and convert them to dict
        object_list = [o.__dict__ for o in getattr(server, f'get_{args.list_object.lower()}s')()]
        sorted_records = sorted(object_list, key=lambda d: d[sort_field])
        if format == 'names':
            for record in sorted_records:
                print(record['name'])
        elif format == 'names_ids':
            for record in sorted_records:
                print(record['name'], record['id'])
        elif format == 'ids_names':
            for record in sorted_records:
                print(record['id'], record['name'])
        elif format == 'names_projects':
            for record in sorted_records:
                print(record['name'], record['project_name'])
        elif format == 'full_df':
            df = pd.DataFrame(sorted_records)
            print(tabulate(df, headers='keys', tablefmt='psql', colalign='left'))
        elif format == 'full_dictionary':
            for record in sorted_records:
                print(record)
        elif format == 'full_dictionary_pretty':
            for record in sorted_records:
                pprint(record)
