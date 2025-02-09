import pandas as pd
from tabulate import tabulate
from pprint import pprint

from tableau_utilities.general.cli_styling import Color, Symbol
from tableau_utilities.tableau_server.tableau_server import TableauServer

# Print Styling
COLOR = Color()
SYMBOL = Symbol()


def server_info(args, server):
    """ Prints information for datasources, projects, or workbooks

    Args:
        args: The args from the CLI
        server (TableauServer): the Tableau Server authentication object

    """

    # Set variables from args
    format = args.list_format
    sort_field = args.list_sort_field
    list_object = args.list_object
    info_export = args.info_export

    if list_object:
        print(f'{COLOR.fg_cyan}{list_object.title()}s:{COLOR.reset}')
        # Get server objects, and convert them to dict
        object_list = [o.__dict__ for o in getattr(server.get, f'{list_object.lower()}s')()]
        sorted_records = sorted(object_list, key=lambda d: d[sort_field])
        df = pd.DataFrame(sorted_records)

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
            print(tabulate(df, headers='keys', tablefmt='psql', colalign='left'))
        elif format == 'full_dictionary':
            for record in sorted_records:
                print(record)
        elif format == 'full_dictionary_pretty':
            for record in sorted_records:
                pprint(record)

        if info_export:
            df.to_csv(f'{info_export}.csv', index=False)

