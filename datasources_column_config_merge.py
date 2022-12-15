import argparse
import json
import os
import shutil
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def do_args():
    """ Parse arguments.

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(description='List columns in Tableau datasources')
    parser.add_argument('-e', '--existing_config',
                        help='The path to the current configuration')
    parser.add_argument('-a', '--additional_config',
                        help='The path to the configuration. This code ASSUMES that the additional config is for a single datasource ')

    return parser.parse_args()


def read_file(file_path):
    with open(file_path, "r") as input_file:
        config = json.load(input_file)

    return config

def write_file(file_path):
    with open(output_file_column_config, "w") as outfile:
        json.dump(column_configs, outfile)


def merge_configs(existing_config, additional_config):

    print(type(existing_config))
    print(type(additional_config))

    print(existing_config)
    print(additional_config)

    new_config = {}

    for column_name, column_details in additional_config.items():
        print('-' * 50)
        # If the column doesn't exist just add it
        if column_name not in existing_config:
            existing_config[column_name] = column_details

            # print('ADDING COLUMN', column_name, column_details)

        elif column_name in existing_config:
            print('ALTERING COLUMN')
            print('EXISTING COLUMN',  column_name, existing_config[column_name])
            print('ADDITIONAL COLUMN', column_name, column_details)

            existing_config[column_name]['description'] = column_details['description']
            existing_config[column_name]['folder'] = column_details['folder']
            existing_config[column_name]['persona'] = column_details['persona']

            datasources_list = []

            for each_datasource in existing_config[column_name]['datasources']:
                if each_datasource['name'] == column_details['datasources'][0]['name']:
                    datasources_list.append(column_details['datasources'][0])
                else:
                    datasources_list.append(each_datasource)

            print('ALTERED COLUMN', column_name, existing_config[column_name])












        # If the column name is in the existing config then
        # 1. Add the description, folder, and persona from the new config
        # 2. Add or overwrite the datasource information in the datasources

        print('-'*20)






if __name__ == '__main__':
    args = do_args()

    # Read files
    existing_config = read_file(args.existing_config)
    additional_config = read_file(args.additional_config)

    # Merge
    merge_configs(existing_config, additional_config)
