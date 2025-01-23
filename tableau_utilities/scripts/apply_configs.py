from copy import deepcopy
import pprint
from typing import Dict, Any, List
from time import time


from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.general.cli_styling import Color, Symbol
from tableau_utilities.general.config_column_persona import personas
from tableau_utilities.scripts.datasource import add_metadata_records_as_columns, create_column
from tableau_utilities.scripts.gen_config import build_configs
from tableau_utilities.scripts.merge_config import read_file

COLOR = Color()
SYMBOL = Symbol()

class ApplyConfigs:
    """Applies a set of configs to a datasource. Configs prefixed with target_ will be applied to the datasource.
    Configs prefixed with datasource_ represent the current state of the datasource before changes.

    Args:
        datasource_name: The name of the datasource.
        datasource_path: The path to the datasource file.
        column_config: The column config to apply to the datasource.
        calculated_field_config: The calculated field config to apply to the datasource.
        debugging_logs: True to print debugging logs to the console

    Returns:
        None
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
        """ Remove all configs except for the columns that match the self.datasource_name

        Args:
            comfig: A tableau column config. Takes both a column config and a calculated column config

        Returns:
            A config with any datasource that is not self.datasource_name removed

        """

        try:
            return config[self.datasource_name]
        except KeyError:
            print(f'{color.fg_red}No matching datasource found in config for {self.datasource_name}{color.reset}')
            return {}

    def invert_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Invert the column config and calc config.
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
            config_A: The first config to prepare.
            config_B: The second config to prepare.

        Returns:
            A single configuration with columns from both configs.
        """

        # invert the configs
        config_A = self.invert_config(config_A)
        config_B = self.invert_config(config_B)

        # Get only the configs to the current datasource.
        # Calculated configs from a datasource can sometimes be empty. If it's empty skip this step
        if len(config_A) > 0:
            config_A = self.select_matching_datasource_config(config_A)

        if len(config_B) > 0:
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
        pp = pprint.PrettyPrinter(indent=4, width=200, depth=None, compact=False)

        for target_entry in target_config:
            if target_entry['caption'] == 'Is Current Month':
                print(target_entry)
        for ds in datasource_config:
            if ds['caption'] == 'Is Current Month':
                print(ds)

        for target_entry in target_config:
            print(target_entry)
            if not any(target_entry == datasource_entry for datasource_entry in datasource_config):
                print(f'{color.fg_yellow}NEED TO MAKE CHANGE:{color.reset}{target_entry}')
                changes_to_make.append(target_entry)

        print(f'{color.fg_yellow}AFTER CREATING CHANGE LIST{color.reset}')
        pp.pprint(changes_to_make)

        print(len(changes_to_make))
        return changes_to_make

    def execute_changes(self, columns_list: List[Dict[str, Any]], datasource):
        """ Applies the config to the datasource and saves the datasource.

        Args:
            columns_list: The list of columns with changes to apply
            datasource: The datasource object to apply the changes to

        Returns:
            None

        """

        print(f'{color.fg_cyan}...Applying Changes to {self.datasource_name}...{color.reset}')

        for each_column in columns_list:
            if self.debugging_logs:
                print(f'{color.fg_yellow}column:{color.reset}{each_column}')
                print(each_column)
                print(each_column['caption'])
                print(type(each_column))

            column = datasource.columns.get(each_column['local-name'])
            persona = personas.get(each_column['persona'].lower(), {})

            # if the column is none then create the column object
            # This will happen when adding new calculation fields
            if not column:
                column = create_column(each_column['local-name'], persona)

            if self.debugging_logs:
                print(f'{color.fg_yellow}column 2025:{color.reset}{column}')

            if self.debugging_logs:
                print(f'{color.fg_yellow}persona:{color.reset}{persona}')

            column.caption = each_column['caption'] or column.caption
            column.role = persona.get('role') or column.role
            column.type = persona.get('role_type') or column.type
            column.datatype = persona.get('datatype') or column.datatype
            column.desc = each_column['description'] or column.desc

            if 'calculation' in each_column:
                column.calculation = each_column['calculation'] or column.calculation

            if self.debugging_logs:
                print(f'{color.fg_yellow}column:{color.reset}{each_column}')

            datasource.enforce_column(column, remote_name=each_column['remote_name'], folder_name=each_column['folder'])

        start = time()
        print(f'{color.fg_cyan}...Saving datasource changes...{color.reset}')
        datasource.save()
        print(f'{color.fg_green}{symbol.success} (Done in {round(time() - start)} sec) '
              f'Saved datasource changes: {color.fg_yellow}{self.datasource_path}{color.reset}')


    def apply_config_to_datasource(self):
        """ Applies a set of configs (column_config and calculated_column_config) to a datasource.
        If a column is in a datasource but NOT in the config that column will be unchanged.
        Columns are not removed via this function such as calculated fields that you may want to remove.
        """

        datasource = Datasource(self.datasource_path)

        # Run column init on the datasource to make sure columns aren't hiding in Metadata records
        datasource = add_metadata_records_as_columns(datasource, self.debugging_logs)
        print(f'{color.fg_cyan}Ran column init {self.datasource_name}...{color.reset}')

        # Build the config dictionaries from the datasource
        datasource_column_config, datasource_calculated_column_config = build_configs(datasource, self.datasource_name)
        print(f'{color.fg_cyan}Built dictionaries from the datasource {self.datasource_name}...{color.reset}')

        # # Prepare the configs by inverting, combining and removing configs for other datasources
        target_config = self.prepare_configs(self.target_column_config, self.target_calculated_column_config)
        print(f'{color.fg_cyan}Prepared the target configs {self.datasource_name}...{color.reset}')

        datasource_config = self.prepare_configs(datasource_column_config, datasource_calculated_column_config)
        print(f'{color.fg_cyan}Prepared the datasource configs {self.datasource_name}...{color.reset}')

        target_config = self.flatten_to_list_of_fields(target_config)
        datasource_config = self.flatten_to_list_of_fields(datasource_config)

        changes_to_make = self.compare_columns(target_config, datasource_config)

        self.execute_changes(changes_to_make, datasource)

def apply_configs(args):
    # Set variables from the args
    debugging_logs = args.debugging_logs
    datasource_name = args.name
    datasource_path = args.file_path

    target_column_config = read_file(args.column_config)
    target_calculated_column_config = read_file(args.calculated_column_config)

    AC = ApplyConfigs(datasource_name, datasource_path, target_column_config, target_calculated_column_config, debugging_logs)

    AC.apply_config_to_datasource()
