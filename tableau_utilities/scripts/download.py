import argparse
import pandas as pd
from tabulate import tabulate
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer
from tableau_utilities.scripts.server_info import object_list_to_dicts


def get_object_id(project_name, object_name, object_list):

    # objects = object_list_to_dicts(object_list)

    return [o['id'] for o in object_list if o['name'] == object_name and  o['project_name'] == project_name][0]

def get_prpject_and_object_names(id, object_list):

    for o in object_list:
        if o['id'] == id:
            return o['name'], o['project_name']


def download_objects(args, server):

    if args.object_type == 'datasource':
        object_list = [d for d in server.get_datasources()]
    if args.object_type == 'workbook':
        object_list = [w for w in server.get_workbooks()]

    object_list = object_list_to_dicts(object_list)

    id = args.id
    object_name = args.name
    project_name = args.project_name

    if id is not None:
        project_name, object_name = get_prpject_and_object_names(id, object_list)

    if id is None:
        id = get_object_id(project_name, object_name, object_list)

    print(f'GETTING OBJECT ID: {id}, OBJECT NAME: {object_name}, PROJECT NAME: {project_name}, INCLUDE EXTRACT {args.include_extract}')

    if args.object_type == 'datasource':
        server.download_datasource(id, include_extract=args.include_extract)
    if args.object_type == 'workbook':
        server.download_workbook(id, include_extract=args.include_extract)



# 'd44388c6-6616-4f80-a4e3-97346cfe67e0