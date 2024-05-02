import json
from tableau_utilities.general.cli_styling import Color, Symbol
from tableau_utilities.scripts.gen_config import load_csv_with_definitions, generate_config


def read_file(file_path):
    """ Read a JSON file to a dictionary

    Args:
        file_path (str): The path of the file to read

    """
    with open(file_path, "r") as infile:
        config: dict = json.load(infile)

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
            print('CONGIG', v)
            print('DATASOURCES', v['datasources'])

        sorted_datasources = sorted(v['datasources'], key=lambda d: d['name'])

        if debugging_logs:
            print('DATASOURCS SORTED', sorted_datasources)

        config[k]['datasources'] = sorted_datasources

    # Sort the top level keys alphabetically
    config = dict(sorted(config.items()))

    return config


def read_merge_write(existing_config_path, additional_config_path, output_config_path, debugging_logs):
    # Print Styling
    color = Color()
    symbol = Symbol()

    # Read files
    existing_config = read_file(existing_config_path)
    additional_config = read_file(additional_config_path)

    # Merge
    new_config = merge_2_configs(existing_config, additional_config, debugging_logs)

    # Sort and write the merged config
    new_config = sort_config(new_config, debugging_logs)
    write_file(file_name=output_config_path, config=new_config, debugging_logs=debugging_logs)

    print(f'{color.fg_yellow}EXISTING CONFIG {symbol.arrow_r} {color.fg_grey}{existing_config_path}{color.reset}')
    print(f'{color.fg_yellow}ADDITIONAL CONFIG {symbol.arrow_r} {color.fg_grey}{additional_config_path}{color.reset}')
    print(f'{color.fg_green}{symbol.success}  MERGED CONFIG {symbol.arrow_r} '
          f'{color.fg_grey}{output_config_path}{color.reset}')


def merge_configs(args, server=None):
    """ Merges 2 configs for datasource automation

    """

    # Set variables from args
    existing_config_path = args.existing_config
    additional_config_path = args.additional_config
    definitions_csv_path = args.definitions_csv
    merge_with = args.merge_with
    file_name = f'{args.merged_config}.json'
    target_directory = args.target_directory
    debugging_logs = args.debugging_logs
    existing_config = args.existing_config

    # Print Styling
    color = Color()
    symbol = Symbol()

    # Merge 2 configs
    if merge_with == 'config':
        read_merge_write(existing_config_path, additional_config_path, file_name, debugging_logs)

    # Merge a config with a definitions csv
    elif merge_with == 'csv':
        # Read files
        existing_config = read_file(existing_config)
        definitions_mapping = load_csv_with_definitions(file=definitions_csv_path)
        # Merge
        new_config = add_definitions_mapping(existing_config, definitions_mapping)
        # Sort and write the merged config
        new_config = sort_config(new_config, debugging_logs)
        write_file(file_name=file_name, config=new_config, debugging_logs=debugging_logs)

        print(f'{color.fg_yellow}EXISTING CONFIG {symbol.arrow_r} '
              f'{color.fg_grey}{existing_config_path}{color.reset}')
        print(f'{color.fg_yellow}ADDITIONAL CONFIG {symbol.arrow_r} '
              f'{color.fg_grey}{additional_config_path}{color.reset}')
        print(f'{color.fg_green}{symbol.success}  MERGED CONFIG {symbol.arrow_r} '
              f'{color.fg_grey}{file_name}{color.reset}')

    elif merge_with == 'generate_merge_all':
        # Generate the configs and return the paths of where they are
        new_column_config_path, new_calculated_column_config_path = generate_config(args, server)
        print(f'{color.fg_green}{symbol.success}  GENERATED CONFIGS {symbol.arrow_r} '
              f'{color.fg_grey}{new_column_config_path} {symbol.sep} {new_calculated_column_config_path}{color.reset}')
        print(f'{color.fg_yellow}TARGET DIRECTORY {symbol.arrow_r} {color.fg_grey}{target_directory}{color.reset}')

        existing_column_config_path = f'{target_directory}column_config.json'
        existing_calc_config_path = f'{target_directory}tableau_calc_config.json'

        read_merge_write(existing_config_path=existing_column_config_path,
                         additional_config_path=new_column_config_path,
                         output_config_path=existing_column_config_path,
                         debugging_logs=debugging_logs)
        read_merge_write(existing_config_path=existing_calc_config_path,
                         additional_config_path=new_calculated_column_config_path,
                         output_config_path=existing_calc_config_path,
                         debugging_logs=debugging_logs)




