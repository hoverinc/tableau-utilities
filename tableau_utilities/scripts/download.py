import argparse
import pandas as pd
from tabulate import tabulate
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def get_object_id(project, name, object_list):

    # return [o['id'] for o in object_list if ['name'] == 'name' ]




def download_objects(args, server):
    if args.list_object == 'datasource':
        object_list = [d for d in server.get_datasources()]
    if args.list_object == 'workbook':
        object_list = [w for w in server.get_workbooks()]

    print_info(object_list, args.list_verbosity, args.list_sort_field)

