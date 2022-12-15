import argparse
import json
import os
import shutil
import sys
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
        print(column_name)
        # if column_name == 'Salesforce Account Id':
        #     print(column_details['description'])
        #     print(len(column_details['description']))



        # If the column doesn't exist add it
        if column_name not in existing_config:
            print("ADDING COLUMN", column_name, column_details)
            existing_config[column_name] = column_details


            # print('ADDING COLUMN', column_name, column_details)

        elif column_name in existing_config:
            print('ALTERING COLUMN:', column_name)
            print('EXISTING COLUMN',  column_name, existing_config[column_name])
            print('ADDITIONAL COLUMN', column_name, column_details)


            if column_name == 'Salesforce Account Id':
                print(column_details['description'])
                print(len(column_details['description']))
                # print('EXITING')
                # sys.exit(0)
        #
        #
        #     print('-' * 50)
        #     print('ALTERING COLUMN:', column_name)
        #     print('EXISTING COLUMN',  column_name, existing_config[column_name])
        #     print('ADDITIONAL COLUMN', column_name, column_details)
        #
        #     if column_name == 'Salesforce Account Current Contract End Date':
        #         sys.exit(0)
        #
        #     # Replace these attributes if there are values in the new configuration
            if len(column_details['description'].strip()) > 0:
                print('CHANGING DESCRIPTION')
                print('DESCRIPTION CURRENT:', existing_config[column_name]['description'])
                print('DESCRIPTION NEW:', column_details['description'])
                existing_config[column_name]['description'] = column_details['description']
                print('DESCRIPTION SET TO:', existing_config[column_name]['description'])
                # print('EXITING')
                # sys.exit(0)

            if len(column_details['folder'].strip()) > 0:
                print('CHANGING FOLDER')
                print('FOLDER CURRENT:', existing_config[column_name]['folder'])
                print('FOLDER NEW:', column_details['folder'])
                existing_config[column_name]['folder'] = column_details['folder']
                print('FOLDER SET TO:', existing_config[column_name]['folder'])
                # print('EXITING')
                # sys.exit(0)

            if len(column_details['persona'].strip()) > 0:
                print('CHANGING PERSONA')
                print('PERSONA CURRENT:', existing_config[column_name]['persona'])
                print('PERSONA NEW:', column_details['persona'])
                existing_config[column_name]['persona'] = column_details['persona']
                print('PERSONA SET TO:', existing_config[column_name]['persona'])

            datasources_list = []
            print('CHANGING DATASOURCES')
            print('DATASOURCES CURRENT:', existing_config[column_name]['datasources'])
            print('DATASOURCES NEW:', column_details['datasources'])

            for each_datasource in existing_config[column_name]['datasources']:
                if each_datasource['name'] == column_details['datasources'][0]['name']:
                    datasources_list.append(column_details['datasources'][0])
                else:
                    datasources_list.append(each_datasource)

            if column_name == 'Salesforce Account Id':
                print('EXITING')
                sys.exit(0)






        #     print('ALTERED COLUMN', column_name, existing_config[column_name])












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
