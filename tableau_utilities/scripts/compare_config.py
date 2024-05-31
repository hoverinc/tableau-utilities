from typing import Dict, Any

# Compare 2 configs and generate a list of adjustments
# do not include the data source piece


UPDATE_ACTIONS = [
    'delete_metadata',
    'modify_metadata',
    'add_metadata',
    'add_column',
    'modify_column',
    'add_folder',
    'delete_folder'
]

class CompareConfigs():
    """ Compares 2 config files and returns a list of changes to make

    Keyword Args:
        snowflake_conn_id (str): The connection ID for Snowflake, used for the datasource connection info
        tableau_conn_id (str): The connection ID for Tableau
        github_conn_id (str): The connection ID for GitHub

    Returns: A dict of tasks to be updated for the datasource.
    """
    def __init__(self, current_config: Dict[str, Any], new_config: Dict[str, Any]) -> None:
        self.current_config = current_config
        self.new_config = new_config

    def __add_task(self, datasource_id, action, action_attrs, log_compare_attrs=None):
        """ Add a task to the dictionary of tasks:
            add_column, modify_column, add_folder, delete_folder, or update_connection

            Sample: {
                "abc123def456": {
                    "datasource_name": "Datasource Name",
                    "project": "Project Name",
                    "add_column": [attrib, attrib],
                    "modify_column": [attrib, attrib]
                    "add_folder": [attrib, attrib]
                    "delete_folder": [attrib, attrib]
                    "update_connection": [attrib, attrib]
                }
            }
        Args:
            datasource_id (str): The ID of the datasource
            action (str): The name of action to do.
            action_attrs (dict): Dict of attributes for the action to use, from the config.
            log_compare_attrs (dict): (Optional) Dict of the attributes to be logged for comparison.
        """
        if action and action not in UPDATE_ACTIONS:
            raise Exception(f'Invalid action {action}')

        if action:
            self.tasks[datasource_id][action].append(action_attrs)
            datasource_name = self.tasks[datasource_id]['datasource_name']
            logging.info(
                '  > (Adding task) %s: %s %s\nAttributes:\n\t%s\n\t%s',
                action, datasource_id, datasource_name, action_attrs, log_compare_attrs
            )

    @staticmethod
    def __get_column_diffs(tds_col, cfg_column):
        """ Compare the column from the tds to attributes we expect.

        Args:
            tds_col (Column): The Tableau Column object from the datasource.
            cfg_column (cfg.CFGColumn): The column from the Config.

        Returns: A dict of differences
        """
        different_value_attrs = dict()
        # If there is no column, either in the Datasource.columns or the config, then return False
        if not tds_col or not cfg_column:
            return different_value_attrs
        # Get a list of attributes that have different values in the Datasource Column vs the config
        cfg_attrs = cfg_column.dict()
        cfg_attrs.pop('folder_name', None)
        cfg_attrs.pop('remote_name', None)
        for attr, value in cfg_attrs.items():
            tds_value = getattr(tds_col, attr)
            if tds_value != value:
                different_value_attrs[attr] = tds_value
        # Return the different attributes
        if different_value_attrs:
            logging.info('  > (Column diffs) %s: %s', cfg_column.caption, different_value_attrs)
        return different_value_attrs

    def __compare_column_metadata(self, datasource_id: str, tds: Datasource, column: cfg.CFGColumn):
        """ Compares the metadata of the Datasource to the config,
            and adds tasks for metadata that needs to be added, modified, or deleted.

        Returns: True if metadata needs to be updated
        """
        metadata_update = False
        if not column.remote_name or column.calculation:
            return metadata_update

        # Add task to delete metadata if the opposite casing version of it exists
        is_upper = column.remote_name.upper() == column.remote_name
        lower_metadata: MetadataRecord = tds.connection.metadata_records.get(column.remote_name.lower())
        if lower_metadata and is_upper:
            self.__add_task(
                datasource_id=datasource_id,
                action='delete_metadata',
                action_attrs={'remote_name': lower_metadata.remote_name},
                log_compare_attrs={'remote_name': column.remote_name}
            )
        is_lower = column.remote_name.lower() == column.remote_name
        upper_metadata: MetadataRecord = tds.connection.metadata_records.get(column.remote_name.upper())
        if upper_metadata and is_lower:
            self.__add_task(
                datasource_id=datasource_id,
                action='delete_metadata',
                action_attrs={'remote_name': upper_metadata.remote_name},
                log_compare_attrs={'remote_name': column.remote_name}
            )

        # Add task to modify the metadata if the local_name does not match
        metadata: MetadataRecord = tds.connection.metadata_records.get(column.remote_name)
        if metadata and metadata.local_name != column.name:
            metadata_update = True
            self.__add_task(
                datasource_id=datasource_id,
                action='modify_metadata',
                action_attrs={'remote_name': metadata.remote_name, 'local_name': column.name},
                log_compare_attrs={'local_name': metadata.local_name}
            )

        # Add task to add the metadata if it doesn't exist
        if not metadata:
            metadata_update = True
            logging.warning('Column metadata does not exist - may be missing in the SQL: %s',
                            column.remote_name)
            metadata_attrs = {
                'conn': {
                    'parent_name': f'[{tds.connection.relation.name}]',
                    'ordinal': len(tds.connection.metadata_records)
                               + len(self.tasks[datasource_id]['add_metadata']),
                },
            }
            metadata_attrs['conn'].update(column.metadata)
            # Set extract attributes if the datasource has an extract
            if tds.extract:
                metadata_attrs['extract'] = {
                    'parent_name': f'[{tds.extract.connection.relation.name}]',
                    'ordinal': len(tds.extract.connection.metadata_records)
                               + len(self.tasks[datasource_id]['add_metadata']),
                    'family': tds.connection.relation.name
                }
                metadata_attrs['extract'].update(column.metadata)
            self.__add_task(datasource_id, 'add_metadata', metadata_attrs)

        return metadata_update

    @staticmethod
    def __compare_column_mapping(tds: Datasource, column: cfg.CFGColumn):
        """ Compares the expected column mapping to the mapping in the Datasource """

        # Mapping is not required when the column is a calculation or remote_name is not provided
        if column.calculation or not column.remote_name:
            return False

        # Mapping is not required when there is no cols section and the local_name is the same as the remote_name
        if not tds.connection.cols and column.name[1:-1] == column.remote_name:
            return False

        parent_name = f'[{tds.connection.relation.name}]'
        # Return True If the column is not already mapped the cols section
        if {'key': column.name, 'value': f'{parent_name}.[{column.remote_name}]'} not in tds.connection.cols:
            return True

        # Return True If the column is mapped in opposite case of the expected key / column name
        if column.name.upper() != column.name:
            opposite_case = column.name.upper()
        else:
            opposite_case = column.name.lower()
        if tds.connection.cols.get(opposite_case):
            return True

        return False

    def __compare_connection(self, dsid, ds_name, tds_connection, expected_attrs):
        """ Compare the connection from the Datasource to attributes we expect.
            If there is a difference, add a task to update the connection.

        Args:
            dsid (str): The Datasource ID.
            ds_name (str): The Datasource name.
            tds_connection (Datasource.connection): The Datasource.connection object.
            expected_attrs (dict): The dict of expected connection attributes.
        """
        named_conn = tds_connection.named_connections[expected_attrs['class_name']]
        tds_conn = tds_connection[expected_attrs['class_name']]
        if not tds_conn:
            logging.warning('Datasource does not have a %s connection: %s',
                            expected_attrs['class_name'], ds_name)
        # Check for a difference between the Datasource connection and the expected connection information
        connection_diff = False
        if expected_attrs['server'] != named_conn.caption:
            connection_diff = True
        for attr, value in expected_attrs.items():
            tds_attr_value = getattr(tds_conn, attr)
            if tds_attr_value and tds_attr_value.lower() != value.lower():
                connection_diff = True
        # Add a task if there is a difference
        if connection_diff:
            self.__add_task(dsid, 'update_connection', expected_attrs, tds_conn.dict())
        else:
            logging.info('  > (No changes needed) Connection: %s', ds_name)

    def __compare_folders(self, datasource_id, tds_folders, cfg_folders):
        """ Compares folders found in the datasource and in the config.
            - If there are folders in the source that are not in the config,
              a task will be added to delete the folder.
            - If there are folders in the config that are not in the datasource,
              a task will be added to add the folder.

        Args:
            tds_folders (Datasource.folders_common): The dict of folders from the Datasource
            cfg_folders (cfg.CFGList[cfg.CFGFolder]): The dict of folders from the Config
        """
        for tds_folder in tds_folders:
            if not cfg_folders.get(tds_folder):
                self.__add_task(datasource_id, 'delete_folder', {'name': tds_folder.name})
        for cfg_folder in cfg_folders:
            if not tds_folders.get(cfg_folder):
                self.__add_task(datasource_id, 'add_folder', {'name': cfg_folder.name})
