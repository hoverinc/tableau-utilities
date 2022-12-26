import argparse
import pandas as pd
from tabulate import tabulate
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def get_object_id(project_name, object_name, object_list):

    return [o['id'] for o in object_list if o['name'] == object_name and  o['project_name'] == project_name]




def download_objects(args, server):

    if args.object_type == 'datasource':
        object_list = [d for d in server.get_datasources()]
    if args.object_type == 'workbook':
        object_list = [w for w in server.get_workbooks()]
        for o in object_list:
            print(o)

    id = args.id
    if id is None:
        id = get_object_id(args.project_name, args.name, object_list)

    print('Object Id', id,)


    print_info(object_list, args.list_verbosity, args.list_sort_field)

