from copy import deepcopy
import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.scripts.gen_config import build_configs
from tableau_utilities.scripts.datasource import add_metadata_records_as_columns

class ApplyConfigs:
    def __init__(self, datasource_name, datasource_path, column_config, calculated_column_config, debugging_logs):
        self.datasource_name = datasource_name
        self.datasource_path = datasource_path
        self.column_config = column_config
        self.calculated_column_config = calculated_column_config
        self.debugging_logs = debugging_logs



    def invert_config(self, config):
        """ Helper function to invert the column config and calc config.
            Output -> {datasource: {column: info}}

        Args:
            iterator (dict): The iterator to append invert data to.
            config (dict): The config to invert.
        """

        temp_config = {}

        for column, i in config.items():
            for datasource in i['datasources']:
                new_info = deepcopy(i)
                del new_info['datasources']
                new_info['local-name'] = datasource['local-name']
                new_info['remote_name'] = datasource['sql_alias'] if 'sql_alias' in datasource else None
                temp_config.setdefault(datasource['name'], {column: new_info})
                temp_config[datasource['name']].setdefault(column, new_info)

        if self.debugging_logs:
            pp = pprint.PrettyPrinter(indent=4, width=80, depth=None, compact=False)
            pp.pprint(temp_config)

        return temp_config

    def combine_configs(self):
        pass

    def prepare_configs(self, config_A, config_B):
        """ Takes 2 configs to invert, combine, and remove irrelevant datasource information. Columns in a main config
        can be in 1 or many Tableau datasources.  So when managing multiple datasources it's likely to have columns that
        need removal

        Args:
            config_A:
            config_B:

        Returns:

        """
        # datasource = self.invert_config(self.column_config)
        # self.invert_config(self.calculated_column_config)
        # combined_config = {**dict1, **dict2}
        pass

    def compare_columns(self):
        """ Compares the config to a datasource. Generates a list of changes to make the datasource match the config

        Returns:
            dict: a dictionary with the columns that need updating

        """

        # compare the caption. If the caption matches compare the attributes
        pass

    def compare_configs(self, config, datasource_cureent_config, datasource_name):
        """ Compares the config to a datasource. Generates a list of changes to make the datasource match the config

        Returns:
            dict: a dictionary with the columns that need updating

        """

        # compare the caption. If the caption matches compare the attributes
        pass


    def execute_changes(self, column_config, calculated_field_config, datasource):
        """ Applies changes to make

        Args:
            config:
            datasource:

        Returns:

        """
        pass

    def apply_config_to_datasource(self):
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

        datasource = Datasource(self.datasource_path)

        # Run column init on the datasource to make sure columns aren't hiding in Metadata records
        datasource = add_metadata_records_as_columns(datasource, self.debugging_logs)

        # Build the config dictionaries from the datasource
        datasource_current_column_config, datasource_current_calculated_column_config = build_configs(datasource,
                                                                                                      self.datasource_name)
        # Prepare the configs by inverting, combining and removing configs for other datasources
        config = self.prepare_configs()
        current_datasource_config = self.prepare_configs()

        # datasource = self.invert_config(self.column_config)
        # self.invert_config(self.calculated_column_config)
        # combined_config = {**dict1, **dict2}



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

    AC = ApplyConfigs(datasource_name, datasource_path, column_config, calculated_column_config, debugging_logs)

    AC.apply_config_to_datasource()