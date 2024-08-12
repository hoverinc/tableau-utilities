from copy import deepcopy
import os
import pprint
import shutil
from typing import Dict, Any, List
from time import time


from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.general.cli_styling import Color, Symbol
from tableau_utilities.general.config_column_persona import personas
from tableau_utilities.scripts.datasource import add_metadata_records_as_columns
from tableau_utilities.scripts.gen_config import build_configs
from tableau_utilities.scripts.merge_config import read_file

color = Color()
symbol = Symbol()

class ApplyConfigs:
    """Applies a set of configs to a datasource. Configs prefixed with target_ will be applied to the datasource.
    Configs prefixed with datasource_ represent the current state of the datasource before changes.
    """

    def __init__(self,
                 datasource_name: str,
                 datasource_path: str,
                 target_column_config: Dict[str, Any],
                 target_calculated_column_config: Dict[str, Any],
                 debugging_logs: bool) -> None:
        self.datasource_name: str = datasource_name
        self.datasource_path: str = datasource_path
        self.target_column_config: Dict[str, Any] = target_column_config
        self.target_calculated_column_config: Dict[str, Any] = target_calculated_column_config
        self.debugging_logs: bool = debugging_logs


    def select_matching_datasource_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """ Limit

        Args:
            comfig:

        Returns:
            A config with any datasource that is not self.datasource_name removed

        """

        config = config[self.datasource_name]
        return config

    def invert_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Helper function to invert the column config and calc config.
        Output -> {datasource: {column: info}}

        Args:
            config (dict): The config to invert.

        Returns:
            dict: The inverted config.
        """

        inverted_config = {}

        for column, i in config.items():
            for datasource in i['datasources']:
                new_info = deepcopy(i)
                del new_info['datasources']
                new_info['local-name'] = datasource['local-name']
                new_info['remote_name'] = datasource['sql_alias'] if 'sql_alias' in datasource else None
                inverted_config.setdefault(datasource['name'], {column: new_info})
                inverted_config[datasource['name']].setdefault(column, new_info)

        if self.debugging_logs:
            pp = pprint.PrettyPrinter(indent=4, width=200, depth=None, compact=False)
            pp.pprint(inverted_config)

        return inverted_config


    def prepare_configs(self, config_A: Dict[str, Any], config_B: Dict[str, Any]) -> Dict[str, Any]:
        """ Takes 2 configs to invert, combine, and remove irrelevant datasource information. Columns in a main config
        can be in 1 or many Tableau datasources.  So when managing multiple datasources it's likely to have columns that
        need removal

        Args:
            config_A:
            config_B:

        Returns:

        """

        # invert the configs
        config_A = self.invert_config(config_A)
        config_B = self.invert_config(config_B)

        # Get only the configs to the current datasource
        config_A = self.select_matching_datasource_config(config_A)
        config_B = self.select_matching_datasource_config(config_B)

        # Combine configs
        combined_config = {**config_A, **config_B}

        if self.debugging_logs:
            print(f'{color.fg_yellow}AFTER COMBINING CONFIGS{color.reset}')
            pp = pprint.PrettyPrinter(indent=4, width=200, depth=None, compact=False)
            pp.pprint(combined_config)

        return combined_config


    def flatten_to_list_of_fields(self, nested_dict: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Flattens a nested dictionary by removing one level of nesting and adding a "Caption" key.

        Args:
            nested_dict (Dict[str, Dict[str, Any]]): The nested dictionary to flatten.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with "Caption" as a key.
        """
        flattened_list = []
        for key, value in nested_dict.items():
            flattened_entry = {"caption": key}
            flattened_entry.update(value)
            flattened_list.append(flattened_entry)

        if self.debugging_logs:
            print(f'{color.fg_yellow}AFTER FLATTENING{color.reset}')
            for field_config in flattened_list:
                print(field_config)

        return flattened_list
    #
    # def merge_configs(self, target_config: List[Dict[str, Any]], datasource_config: List[Dict[str, Any]]) -> List[
    #     Dict[str, Any]]:
    #     """Merges two lists of dictionaries ensuring all 'local-name' from both lists are present.
    #
    #     If the 'local-name' is in both lists, the values from `target_config` are used.
    #
    #     Args:
    #         target_config (List[Dict[str, Any]]): The target configuration list of dictionaries.
    #         datasource_config (List[Dict[str, Any]]): The datasource configuration list of dictionaries.
    #
    #     Returns:
    #         List[Dict[str, Any]]: A merged list of dictionaries.
    #     """
    #     merged_dict = {}
    #
    #     # Add all entries from datasource_config to merged_dict
    #     for entry in datasource_config:
    #         local_name = entry['local-name']
    #         merged_dict[local_name] = entry
    #
    #     # Update or add entries from target_config to merged_dict
    #     for entry in target_config:
    #         local_name = entry['local-name']
    #         merged_dict[local_name] = entry
    #
    #     # Convert merged_dict back to a list of dictionaries
    #     merged_list = list(merged_dict.values())
    #
    #     # if self.debugging_logs:
    #     print(f'{color.fg_yellow}AFTER MERGING{color.reset}')
    #     for field_config in merged_list:
    #         print(field_config)
    #
    #     return merged_list


    def compare_columns(self, target_config: List[Dict[str, Any]], datasource_config: List[Dict[str, Any]]) -> List[
        Dict[str, Any]]:
        """Compares the target config to the datasource config and generates a list of changes to make the datasource match the target config.

        Args:
            target_config (List[Dict[str, Any]]): The target configuration list of dictionaries.
            datasource_config (List[Dict[str, Any]]): The datasource configuration list of dictionaries.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with the columns that need updating.
        """
        changes_to_make = []

        for target_entry in target_config:
            if target_entry not in datasource_config:
                changes_to_make.append(target_entry)

        print(f'{color.fg_yellow}AFTER MERGING{color.reset}')
        for field_config in changes_to_make:
            print(field_config)

        return changes_to_make

    def execute_changes(self, columns_list: List[Dict[str, Any]], datasource):
        """ Applies changes to make

        Args:
            columns_list:
            datasource:

        Returns:

        """

        print(f'{color.fg_cyan}...Applying Changes to {self.datasource_name}...{color.reset}')

        for each_column in columns_list:
            if self.debugging_logs:
                print(f'{color.fg_yellow}column:{color.reset}{each_column}')

            #

            column = datasource.columns.get(each_column['local-name'])

            persona = personas.get(each_column['persona'].lower(), {})

            if self.debugging_logs:
                print(f'{color.fg_yellow}persona:{color.reset}{persona}')

            column.caption = each_column['caption'] or column.caption
            column.role = persona.get('role') or column.role
            column.type = persona.get('role_type') or column.type
            column.datatype = persona.get('datatype') or column.datatype
            column.desc = each_column['description'] or column.desc

            if 'calculation' in each_column:
                column.calculation = each_column['calculation']

            if self.debugging_logs:
                print(f'{color.fg_yellow}column:{color.reset}{each_column}')

            datasource.enforce_column(column, remote_name=each_column['remote_name'], folder_name=each_column['folder'])

        start = time()
        print(f'{color.fg_cyan}...Saving datasource changes...{color.reset}')
        datasource.save()
        print(f'{color.fg_green}{symbol.success} (Done in {round(time() - start)} sec) '
              f'Saved datasource changes: {color.fg_yellow}{self.datasource_path}{color.reset}')

        # start = time()
        # print(f'{color.fg_cyan}...Extracting {self.datasource_name}...{color.reset}')
        # save_folder = f'{self.datasource_name} - AFTER'
        # os.makedirs(save_folder, exist_ok=True)
        # if datasource.extension == 'tds':
        #     xml_path = os.path.join(save_folder, self.datasource_name)
        #     shutil.copy(self.datasource_path, xml_path)
        # else:
        #     xml_path = datasource.unzip(extract_to=save_folder, unzip_all=True)
        # if self.debugging_logs:
        #     print(f'{color.fg_green}{symbol.success} (Done in {round(time() - start)} sec) '
        #           f'AFTER - TDS SAVED TO: {color.fg_yellow}{xml_path}{color.reset}')


    def apply_config_to_datasource(self):
        """ Applies a set of configs (column_config and calculated_column_config) to a datasource.
        If a column is in a datasource but NOT in the config that column will be unchanged.

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
        datasource_column_config, datasource_calculated_column_config = build_configs(datasource, self.datasource_name)

        # if self.debugging_logs:
        #     print('Target Column Config:', self.target_column_config)
        #     print('Target Column Config:', type(self.target_column_config))
        #     print('Target Calculated Column Config:', self.target_calculated_column_config)
        #     # print('Datasource Column Config:', datasource_column_config)

        # Prepare the configs by inverting, combining and removing configs for other datasources
        target_config = self.prepare_configs(self.target_column_config, self.target_calculated_column_config)
        datasource_config = self.prepare_configs(datasource_column_config, datasource_calculated_column_config)

        target_config = self.flatten_to_list_of_fields(target_config)
        datasource_config = self.flatten_to_list_of_fields(datasource_config)

        # merged_config = self.merge_configs(target_config, datasource_config)
        changes_to_make = self.compare_columns(target_config, datasource_config)

        self.execute_changes(changes_to_make, datasource)

        #
        #
        #
        # # Get the changes to make for the column config
        # # Get the changes to make for the calculation config
        #
        # # Apply the changes for the column config
        # # Apply the changes for the calc config
        #
        # # Clean up the empty folders
        #
        # # Save the file
        # pass


def apply_configs(args):
    # Set variables from the args
    debugging_logs = args.debugging_logs
    datasource_name = args.name
    datasource_path = args.file_path
    target_column_config = read_file(args.column_config)
    target_calculated_column_config = read_file(args.calculated_column_config)

    AC = ApplyConfigs(datasource_name, datasource_path, target_column_config, target_calculated_column_config, debugging_logs)

    AC.apply_config_to_datasource()
