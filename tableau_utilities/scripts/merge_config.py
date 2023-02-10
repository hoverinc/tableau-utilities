import argparse
import json
import os
import shutil
import sys
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer
from tableau_utilities.scripts.gen_config import load_csv_with_definitions, generate_config


def read_file(file_path):
    """ Read a JSON file to a dictionary

    Args:
        file_path: The path of the file to read

    """
    with open(file_path, "r") as infile:
        config = json.load(infile)

    return config


def write_file(file_name, config, debugging_logs=False):
    """ Write a dictionary to a JSON file

    Args:
        file_name: The name of the file to write
        config: The dictionary to write to the file

    """
    with open(file_name, "w") as outfile:
        json.dump(config, outfile)

    if debugging_logs:
        print('CONFIG PATH:', file_name)


def add_definitions_mapping(config, definitions_mapping):
    """ Adds definitions from a mapping to the config. Chooses the definition from the mapping if needed

    Args:
        config: A datasource config
        definitions_mapping: A mapping of columns to definitions

    """

    for column, definition in definitions_mapping.items():

        if len(definition) > 0 and column in config:
            config[column]['description'] = definition

    return config


def merge_2_configs(existing_config, additional_config, debugging_logs=False):
    """ Takes 2 configs and adds information from the additional_cong to the existing_config
    The output of the merged config should  be merged into the existing config in a PR
    This assumes that the user is merging the same type of config

    Args:
        existing_config: The current existing config
        additional_config: The additional config to add

    """

    for column_name, column_details in additional_config.items():

        if debugging_logs:
            print(column_name)

        # If the column doesn't exist add it
        if column_name not in existing_config:
            if debugging_logs:
                print("ADDING COLUMN", column_name, column_details)
            existing_config[column_name] = column_details

        elif column_name in existing_config:
            if debugging_logs:
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

                if len(calculation_new.strip()) > 0:
                    if debugging_logs:
                        print('CHANGING CALCULATION')
                        print('CALCULATION CURRENT:', calculation_current)
                        print('CALCULATION NEW:', calculation_new)

                    existing_config[column_name]['calculation'] = calculation_new

                    if debugging_logs:
                        print('CALCULATION SET TO:', existing_config[column_name]['calculation'])

            # Replace these attributes if there are values in the new configuration

            if (isinstance(description_new, list) and len(description_new) > 0) \
                    or (isinstance(description_new, str) and len(description_new.strip()) > 0):
                if debugging_logs:
                    print('CHANGING DESCRIPTION')
                    print('DESCRIPTION CURRENT:', description_current)
                    print('DESCRIPTION NEW:', description_new)

                existing_config[column_name]['description'] = description_new

                if debugging_logs:
                    print('DESCRIPTION SET TO:', existing_config[column_name]['description'])

            if folder_new is not None and len(folder_new.strip()) > 0:
                if debugging_logs:
                    print('CHANGING FOLDER')
                    print('FOLDER CURRENT:', folder_current)
                    print('FOLDER NEW:', folder_new)

                existing_config[column_name]['folder'] = folder_new

                if debugging_logs:
                    print('FOLDER SET TO:', existing_config[column_name]['folder'])

            if len(persona_new.strip()) > 0:
                if debugging_logs:
                    print('CHANGING PERSONA')
                    print('PERSONA CURRENT:', persona_current)
                    print('PERSONA NEW:', persona_new)

                existing_config[column_name]['persona'] = persona_new

                if debugging_logs:
                    print('PERSONA SET TO:', existing_config[column_name]['persona'])

            datasources_list = []

            if debugging_logs:
                print('CHANGING DATASOURCES')
                print('DATASOURCES CURRENT:', datasources_current)
                print('DATASOURCES NEW:', datasources_new)

            datasources_names_current = [d['name'] for d in datasources_current]

            if debugging_logs:
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

            if debugging_logs:
                print('DATASOURCES SET TO:', existing_config[column_name]['datasources'])

    return existing_config


def read_merge_write(existing_config_path, additional_config_path, output_config_path, debugging_logs):
    # Read files
    existing_config = read_file(existing_config_path)
    additional_config = read_file(additional_config_path)

    # Merge
    new_config = merge_2_configs(existing_config, additional_config, debugging_logs)

    # Sort and write the merged config
    new_config = dict(sorted(new_config.items()))
    write_file(file_name=output_config_path, config=new_config, debugging_logs=debugging_logs)

    print(f'EXISTING CONFIG: {existing_config_path}')
    print(f'ADDITIONAL CONFIG: {additional_config_path}')
    print(f'MERGED CONFIG: {output_config_path}')


def merge_configs(args, server=None):
    """ Merges 2 configs for datasource automation

    """

    # Set Arguments
    existing_config_path = args.existing_config
    additional_config_path = args.additional_config
    definitions_csv_path = args.definitions_csv
    merge_with = args.merge_with
    file_name = f'{args.merged_config}.json'
    target_directory = args.target_directory
    debugging_logs = args.debugging_logs

    # Merge 2 configs
    if merge_with == 'config':
        read_merge_write(existing_config_path, additional_config_path, file_name, debugging_logs)

    # Merge a config with a definitions csv
    elif merge_with == 'csv':
        # Read files
        existing_config = read_file(args.existing_config)
        definitions_mapping = load_csv_with_definitions(file=definitions_csv_path)

        # Merge
        new_config = add_definitions_mapping(existing_config, definitions_mapping)

        # Sort and write the merged config
        new_config = dict(sorted(new_config.items()))
        write_file(file_name=file_name, config=new_config, debugging_logs=args.debugging_logs)

        print(f'EXISTING CONFIG: {existing_config_path}')
        print(f'ADDITIONAL CONFIG: {additional_config_path}')
        print(f'MERGED CONFIG: {file_name}')

    elif merge_with == 'generate_merge_all':
        # Generate the configs and return the paths of where they are
        new_column_config_path, new_calculated_column_config_path = generate_config(args, server)
        print('GENERATED CONFIGS TO', new_column_config_path, new_calculated_column_config_path)
        print('TARGET DIRECTORY', target_directory)

        existing_column_config_path = f'{target_directory}column_config.json'
        existing_calc_config_path = f'{target_directory}tableau_calc_config.json'

        print(existing_calc_config_path, existing_column_config_path)

        read_merge_write(existing_config_path=existing_column_config_path,
                         additional_config_path=new_column_config_path,
                         output_config_path=existing_column_config_path,
                         debugging_logs=debugging_logs)
        read_merge_write(existing_config_path=existing_calc_config_path,
                         additional_config_path=new_calculated_column_config_path,
                         output_config_path=existing_calc_config_path,
                         debugging_logs=debugging_logs)




