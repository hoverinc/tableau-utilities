from tableau_utilities.scripts.gen_config import build_configs


def compare_configs(config, datasource_cureent_config, datasource_name):
    """ Compares the config to a datasource. Generates a list of changes to make the datasource match the config

    Returns:
        dict: a dictionary with the columns that need updating

    """
    pass


def execute_changes(column_config, calculated_field_config, datasource):
    """ Applies changes to make

    Args:
        config:
        datasource:

    Returns:

    """
    pass

def apply_config_to_datasource(datasource_name, datasource_path, column_config, calculated_field_config, debugging_logs):
    """ Applies changes to make

    Args:
        datasource_name:
        datasource_path:
        column_config:
        calculated_field_config:
        debugging_logs:



    Returns:
        None

    """

    # Build the config dictionaries from the datasource
    datasource_current_column_config, datasource_current_calculated_column_config = build_configs(datasource_path,
                                                                                                  datasource_name)



    # Get the changes to make for the column config
    # Get the changes to make for the calculation config

    # Apply the changes for the column config
    # Apply the changes for the calc config

    # Clean up the empty folders

    # Save the file
    pass


def apply_configs(args):
    # Set variables from the args
    debugging_logs = args.debugging_logs
    datasource_name = args.name
    datasource_path = args.file_path
    column_config = args.column_config
    calculated_column_config = args.calculated_column_config

    apply_config_to_datasource()
