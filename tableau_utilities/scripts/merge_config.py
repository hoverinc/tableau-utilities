import json
import os
from tableau_utilities.general.cli_styling import Color, Symbol
from tableau_utilities.scripts.gen_config import load_csv_with_definitions, generate_config

# Print Styling
COLOR = Color()
SYMBOL = Symbol()

def read_file(file_path):
    """Read a JSON file to a dictionary.

    Args:
        file_path (str): The path of the file to read.

    Returns:
        dict: The JSON content as a dictionary.
    """

    try:
        with open(file_path, "r") as infile:
            config = json.load(infile)
            print(f"Successfully read file: {file_path}")
            return config
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from file: {file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return {}


def write_file(file_name, config, debugging_logs=False):
    """ Write a dictionary to a JSON file

    Args:
        file_name: The name of the file to write
        config: The dictionary to write to the file

    """

    # Ensure the file_name is an absolute path
    file_name = os.path.abspath(file_name)

    with open(file_name, "w") as outfile:
        json.dump(config, outfile)

    if debugging_logs:
        print('CONFIG PATH:', file_name)


def add_definitions_mapping_any_local_name(config, definitions_mapping):
    """Adds definitions from a mapping to the config. Chooses the definition from the mapping if needed.

    Args:
        config (dict): A datasource config.
        definitions_mapping (dict): A dictionary with columns as keys and their definitions as values.

    Returns:
        dict: The updated config with new descriptions.
    """
    if not isinstance(definitions_mapping, dict):
        raise TypeError("definitions_mapping should be a dictionary")

    for column, definition in definitions_mapping.items():
        if len(definition) > 0:
            column_lower = column.lower()
            for key, value in config.items():
                for datasource in value.get('datasources', []):
                    if datasource.get('local-name', '').lower() == column_lower:
                        config[key]['description'] = definition
                        break
    return config


def merge_2_configs(existing_config, additional_config, debugging_logs=False):
    """ Takes 2 configs and adds information from the additional_cong to the existing_config
    The output of the merged config should  be merged into the existing config in a PR
    This assumes that the user is merging the same type of config

    Args:
        existing_config (dict): The current existing config
        additional_config (dict): The additional config to add
        debugging_logs (bool): True to print debugging logs

    """
    for column_name, column_details in additional_config.items():
        # If the column doesn't exist add it
        if column_name not in existing_config:
            if debugging_logs:
                print("ADDING COLUMN", column_name, column_details)
            existing_config[column_name] = column_details
        else:
            if debugging_logs:
                print('ALTERING COLUMN:', column_name)
                print('\tEXISTING DETAIL:', existing_config[column_name])
                print('\tADDITIONAL DETAIL:', column_details)

            # Replace existing config attributes
            for attribute_name, attribute_value in column_details.items():
                if str(attribute_value).strip() not in ['', 'None'] and attribute_name != 'datasources':
                    if debugging_logs:
                        print(
                            '\tUPDATING ATTRIBUTE', attribute_name,
                            'FROM:', existing_config[column_name][attribute_name],
                            '-> TO:', attribute_value
                        )
                    existing_config[column_name][attribute_name] = attribute_value
            # Remove the existing column datasource if it is included from the additional config
            existing_config[column_name]['datasources'] = [
                d for d in existing_config[column_name]['datasources']
                if d['name'] not in [d['name'] for d in column_details['datasources']]
            ]
            # Add all datasources from the additional column to existing column datasources
            for datasource in column_details['datasources']:
                if debugging_logs:
                    print('\tADDING DATASOURCE:', datasource)
                existing_config[column_name]['datasources'].append(datasource)

            if debugging_logs:
                print('\tCOLUMN ATTRIBUTES SET TO:', existing_config[column_name])

    return existing_config


def sort_config(config, debugging_logs):
    """ Takes a config and returns it sorted by key and with the datasources list sorted

    Args:
        config (dict): A datasource config
        debugging_logs: Prints info to console if true

    Returns:
        config (dict): With the list of datasources sorted.  With the config sorted by column_name (top key)

    """

    # Sort the list of datasources alphabetically
    for k, v in config.items():

        if debugging_logs:
            print('KEY', k)
            print('CONFIG', v)
            print('DATASOURCES', v['datasources'])

        sorted_datasources = sorted(v['datasources'], key=lambda d: d['name'])

        if debugging_logs:
            print('DATASOURCS SORTED', sorted_datasources)

        config[k]['datasources'] = sorted_datasources

    # Sort the top level keys alphabetically
    config = dict(sorted(config.items()))

    return config


def read_merge_write(existing_config_path, additional_config_path, output_config_path, debugging_logs):

    # Read files
    existing_config = read_file(existing_config_path)
    additional_config = read_file(additional_config_path)

    # Merge
    new_config = merge_2_configs(existing_config, additional_config, debugging_logs)

    # Sort and write the merged config
    new_config = sort_config(new_config, debugging_logs)
    write_file(file_name=output_config_path, config=new_config, debugging_logs=debugging_logs)

    print(f'{COLOR.fg_yellow}EXISTING CONFIG {SYMBOL.arrow_r} {COLOR.fg_grey}{existing_config_path}{COLOR.reset}')
    print(f'{COLOR.fg_yellow}ADDITIONAL CONFIG {SYMBOL.arrow_r} {COLOR.fg_grey}{additional_config_path}{COLOR.reset}')
    print(f'{COLOR.fg_green}{SYMBOL.success}  MERGED CONFIG {SYMBOL.arrow_r} '
          f'{COLOR.fg_grey}{output_config_path}{COLOR.reset}')


def merge_configs(args, server=None):
    """ Merges 2 configs for datasource automation

    """

    # Set variables from args
    existing_config_path = args.existing_config
    additional_config_path = args.additional_config
    definitions_csv_path = args.definitions_csv
    merge_with = args.merge_with
    target_directory = args.target_directory
    debugging_logs = args.debugging_logs
    existing_config = args.existing_config
    merged_config_path = args.merged_config


    # Merge 2 configs
    if merge_with == 'config':
        read_merge_write(existing_config_path=existing_config_path,
                         additional_config_path=additional_config_path,
                         output_config_path=merged_config_path,
                         debugging_logs=debugging_logs)

    # Merge a config with a definitions csv.
    elif merge_with == 'csv':
        # Log paths
        if debugging_logs:
            print(f'{COLOR.fg_yellow}EXISTING CONFIG PATH {SYMBOL.arrow_r} '
                  f'{COLOR.fg_grey}{existing_config}{COLOR.reset}')
            print(f'{COLOR.fg_yellow}DEFINITIONS CSV PATH {SYMBOL.arrow_r} '
                  f'{COLOR.fg_grey}{definitions_csv_path}{COLOR.reset}')

        # Read files
        existing_config_content = read_file(existing_config)
        definitions_mapping = load_csv_with_definitions(file=definitions_csv_path, debugging_logs=debugging_logs)

        # Merge
        new_config = add_definitions_mapping_any_local_name(existing_config_content, definitions_mapping)
        
        # Sort and write the merged config
        new_config = sort_config(new_config, debugging_logs)

        write_file(file_name=existing_config, config=new_config, debugging_logs=debugging_logs)

        print(f'{COLOR.fg_yellow}DEFINITIONS CSV {SYMBOL.arrow_r} '
              f'{COLOR.fg_grey}{definitions_csv_path}{COLOR.reset}')
        print(f'{COLOR.fg_yellow}EXISTING CONFIG {SYMBOL.arrow_r} '
              f'{COLOR.fg_grey}{existing_config_path}{COLOR.reset}')
        print(f'{COLOR.fg_yellow}ADDITIONAL CONFIG {SYMBOL.arrow_r} '
              f'{COLOR.fg_grey}{additional_config_path}{COLOR.reset}')
        print(f'{COLOR.fg_green}{SYMBOL.success}  MERGED CONFIG {SYMBOL.arrow_r} '
              f'{COLOR.fg_grey}{existing_config}{COLOR.reset}')

    elif merge_with == 'generate_merge_all':
        # Generate the configs and return the paths of where they are
        new_column_config_path, new_calculated_column_config_path = generate_config(args, server)
        print(f'{COLOR.fg_green}{SYMBOL.success}  GENERATED CONFIGS {SYMBOL.arrow_r} '
              f'{COLOR.fg_grey}{new_column_config_path} {SYMBOL.sep} {new_calculated_column_config_path}{COLOR.reset}')
        print(f'{COLOR.fg_yellow}TARGET DIRECTORY {SYMBOL.arrow_r} {COLOR.fg_grey}{target_directory}{COLOR.reset}')

        # Set the full path for the target directory for files to merge with and output to
        # This overrides the working directory setting where temporary files are stored
        existing_column_config_path = os.path.join(target_directory, 'column_config.json')
        existing_calc_config_path = os.path.join(target_directory, 'tableau_calc_config.json')

        read_merge_write(existing_config_path=existing_column_config_path,
                         additional_config_path=new_column_config_path,
                         output_config_path=existing_column_config_path,
                         debugging_logs=debugging_logs)
        read_merge_write(existing_config_path=existing_calc_config_path,
                         additional_config_path=new_calculated_column_config_path,
                         output_config_path=existing_calc_config_path,
                         debugging_logs=debugging_logs)




