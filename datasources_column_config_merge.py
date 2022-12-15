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
    """ Takes 2 configs and adds information from the additional_cong to the existing_config
    The output of the merged config should  be merged into the existing config in a PR
    This assumes that the user is merging the same type of config

    Args:
        existing_config: The current existing config
        additional_config: The additional config to add

    """

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

            # Set the values from the dictionaries
            description_current = existing_config[column_name]['description']
            description_new = column_details['description']
            folder_current = existing_config[column_name]['folder']
            folder_new = column_details['folder']
            persona_current = existing_config[column_name]['persona']
            persona_new = column_details['persona']
            datasources_current = existing_config[column_name]['datasources']
            datasources_new = column_details['datasources']

            # Only used when it's a calculation config
            if 'calculation' in column_details:
                calculation_current = existing_config[column_name]['calculation']
                calculation_new = column_details['calculation']

                if len(column_details['calculation'].strip()) > 0:
                    print('CHANGING CALCULATION')
                    print('CALCULATION CURRENT:', calculation_current)
                    print('CALCULATION NEW:', calculation_new)
                    existing_config[column_name]['description'] = calculation_new
                    print('CALCULATION SET TO:', existing_config[column_name]['calculation'])

            # Replace these attributes if there are values in the new configuration
            if len(column_details['description'].strip()) > 0:
                print('CHANGING DESCRIPTION')
                print('DESCRIPTION CURRENT:', description_current)
                print('DESCRIPTION NEW:', description_new)
                existing_config[column_name]['description'] = description_new
                print('DESCRIPTION SET TO:', existing_config[column_name]['description'])
                # print('EXITING')
                # sys.exit(0)

            if len(column_details['folder'].strip()) > 0:
                print('CHANGING FOLDER')
                print('FOLDER CURRENT:', folder_current)
                print('FOLDER NEW:', folder_new)
                existing_config[column_name]['folder'] = folder_new
                print('FOLDER SET TO:', existing_config[column_name]['folder'])
                # print('EXITING')
                # sys.exit(0)

            if len(column_details['persona'].strip()) > 0:
                print('CHANGING PERSONA')
                print('PERSONA CURRENT:', persona_current)
                print('PERSONA NEW:', persona_new)
                existing_config[column_name]['persona'] = persona_new
                print('PERSONA SET TO:', existing_config[column_name]['persona'])

            datasources_list = []
            print('CHANGING DATASOURCES')
            print('DATASOURCES CURRENT:', datasources_current)
            print('DATASOURCES NEW:', datasources_new)

            datasources_names_current = [d['name'] for d in datasources_current]
            print(datasources_names_current)

            # If the config has the datasource take the new one otherwise keep all datasources
            for each_datasource in datasources_current:
                if each_datasource['name'] == datasources_new[0]['name']:
                    datasources_list.append(column_details['datasources'][0])
                else:
                    datasources_list.append(each_datasource)

            # Add the new datasource if it's not in the existing things at all
            if datasources_new[0]['name'] not in datasources_names_current:
                datasources_list.append(column_details['datasources'][0])

            existing_config[column_name]['datasources'] = datasources_list
            print('DATASOURCES SET TO:', existing_config[column_name]['datasources'])

            # if column_name == 'Salesforce Account Id':
            #     print('EXITING')
            #     sys.exit(0)






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
