
class TableauDatasourceTasks(models.BaseOperator):
    """ Compares config files to the published datasource,
        to get a dictionary of tasks needing to be updated.

    Keyword Args:
        snowflake_conn_id (str): The connection ID for Snowflake, used for the datasource connection info
        tableau_conn_id (str): The connection ID for Tableau
        github_conn_id (str): The connection ID for GitHub

    Returns: A dict of tasks to be updated for the datasource.
    """
    def __init__(self, *args, **kwargs):
        self.snowflake_conn_id = kwargs.pop('snowflake_conn_id', 'gcp_snowflake_default')
        self.tableau_conn_id = kwargs.pop('tableau_conn_id', None)
        self.github_conn_id = kwargs.pop('github_conn_id', None)
        super().__init__(*args, **kwargs)
        # Set on execution
        self.tasks = dict()

    def __set_connection_attributes(self):
        """ Sets attributes of the datasource connection. """
        snowflake_hook = SnowflakeHook(self.snowflake_conn_id)

        return {
            'class_name': 'snowflake',
            'dbname': snowflake_hook.database,
            'schema': snowflake_hook.schema,
            'server': f'{snowflake_hook.account}.snowflakecomputing.com',
            'service': snowflake_hook.role,
            'username': snowflake_hook.user,
            'warehouse': snowflake_hook.warehouse
        }

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

    def execute(self, context):
        """ Update Tableau datasource according to config. """

        github_conn = BaseHook.get_connection(self.github_conn_id)
        config = cfg.Config(
            githup_token=github_conn.password,
            repo_name=github_conn.extra_dejson.get('repo_name'),
            repo_branch=github_conn.extra_dejson.get('repo_branch'),
            subfolder=github_conn.extra_dejson.get('subfolder')
        )

        ts = get_tableau_server(self.tableau_conn_id)
        expected_conn_attrs = self.__set_connection_attributes()

        # Get the ID for each datasource in the config
        for ds in ts.get.datasources():
            if ds not in config.datasources:
                continue
            config.datasources[ds].id = ds.id

        for datasource in config.datasources:
            if not datasource.id:
                logging.error('!! Datasource not found in Tableau Online: %s / %s',
                              datasource.project_name, datasource.name)
                continue
            dsid = datasource.id
            # Set default dict attributes for tasks, for each datasource
            self.tasks[dsid] = {a: [] for a in UPDATE_ACTIONS}
            self.tasks[dsid]['project'] = datasource.project_name
            self.tasks[dsid]['datasource_name'] = datasource.name
            if not config.in_maintenance_window and AIRFLOW_ENV not in ['STAGING', 'DEV']:
                self.tasks[dsid]['skip'] = 'Outside maintenance window'
                logging.info('(SKIP) Outside maintenance window: %s', datasource.name)
                continue
            elif datasource.name in EXCLUDED_DATASOURCES:
                self.tasks[dsid]['skip'] = 'Marked to exclude'
                logging.info('(SKIP) Marked to exclude: %s', datasource.name)
                continue
            logging.info('Checking Datasource: %s', datasource.name)
            # Download the Datasource for comparison
            dl_path = f"downloads/{dsid}/"
            os.makedirs(dl_path, exist_ok=True)
            ds_path = ts.download.datasource(dsid, file_dir=dl_path, include_extract=False)
            tds = Datasource(ds_path)
            # Cleanup downloaded file after assigning the Datasource
            shutil.rmtree(dl_path, ignore_errors=True)
            # Add connection task, if there is a difference
            self.__compare_connection(dsid, datasource.name, tds.connection, expected_conn_attrs)
            # Add folder tasks, if folders need to be added/deleted
            self.__compare_folders(dsid, tds.folders_common, datasource.folders)
            # Add Column tasks, if there are missing columns, or columns need to be updated
            for column in datasource.columns:
                # Check if the column metadata needs to be updated
                self.__compare_column_metadata(dsid, tds, column)
                # Check if the column needs mapping
                column_needs_mapping = self.__compare_column_mapping(tds, column)
                # Check the column for updates
                tds_column: Column = tds.columns.get(column.name)
                column_diffs: dict = self.__get_column_diffs(tds_column, column)
                tds_folder: Folder = tds.folders_common.get(column.folder_name)
                not_in_folder: bool = tds_folder is None or tds_folder.folder_item.get(column.name) is None
                if not tds_column:
                    self.__add_task(dsid, action='add_column', action_attrs=column.dict())
                elif column_diffs or not_in_folder or column_needs_mapping:
                    self.__add_task(
                        dsid,
                        action='modify_column',
                        action_attrs=column.dict(),
                        log_compare_attrs={
                            'column_diffs': column_diffs,
                            'not_in_folder': not_in_folder,
                            'column_needs_mapping': column_needs_mapping
                        }
                    )
                else:
                    logging.info('  > (No changes needed) Column: %s / %s', datasource.name, column.caption)

        return self.tasks
