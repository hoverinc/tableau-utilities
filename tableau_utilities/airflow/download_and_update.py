
class TableauDatasourceUpdate(models.BaseOperator):
    """ Downloads the datasource.
        Makes all necessary updates to the datasource.
        Publishes the datasource.

    Keyword Args:
        tasks_task_id (str): The task_id of the task that ran the TableauDatasourceTasks operator.
        snowflake_conn_id (str): The connection ID for Snowflake.
        tableau_conn_id (str): The Tableau connection ID
    """
    def __init__(self, *args, **kwargs):
        self.tasks_task_id = kwargs.pop('tasks_task_id')
        self.tableau_conn_id = kwargs.pop('tableau_conn_id', 'tableau_default')
        self.snowflake_conn_id = kwargs.pop('snowflake_conn_id', 'gcp_snowflake_default')
        super().__init__(*args, **kwargs)

    @staticmethod
    def __has_tasks_to_do(tasks):
        """ Check if there are any tasks to be done

        Args:
            tasks (dict): The tasks to be done
        """
        for attributes in tasks.values():
            if isinstance(attributes, list) and attributes:
                return True
        return False

    def __do_action(self, tasks, tds, action):
        """ Executes the action, for each item to do for that action

        Args:
            tasks (dict): The dict of tasks to be done
            tds (Datasource): The Tableau Datasource object
            action (str): The name of the action to be done
        """
        for attrs in tasks[action]:
            logging.info('  > (Update) %s: %s', action, attrs)
            if action == 'modify_metadata':
                tds.connection.metadata_records[attrs['remote_name']].local_name = attrs['local_name']
                if tds.extract:
                    tds.extract.connection.metadata_records[attrs['remote_name']].local_name = attrs['local_name']
            if action == 'delete_metadata':
                tds.connection.metadata_records.delete(attrs['remote_name'])
                if tds.extract:
                    tds.extract.connection.metadata_records.delete(attrs['remote_name'])
            if action == 'add_metadata':
                tds.connection.metadata_records.add(MetadataRecord(**attrs['conn']))
                if tds.extract:
                    tds.extract.connection.metadata_records.add(MetadataRecord(**attrs['extract']))
            if action in ['modify_column', 'add_column']:
                folder_name: str = attrs.pop('folder_name', None)
                remote_name: str = attrs.pop('remote_name', None)
                # Delete existing column mapping
                upper_mapping_key = attrs['name'].upper()
                lower_mapping_key = attrs['name'].lower()
                #   Connection cols mapping
                if tds.connection.cols:
                    if tds.connection.cols.get(upper_mapping_key):
                        tds.connection.cols.delete(upper_mapping_key)
                    if tds.connection.cols.get(lower_mapping_key):
                        tds.connection.cols.delete(lower_mapping_key)
                #   Extract cols mapping
                if tds.extract and tds.extract.connection.cols:
                    if tds.extract.connection.cols.get(upper_mapping_key):
                        tds.extract.connection.cols.delete(upper_mapping_key)
                    if tds.extract.connection.cols.get(lower_mapping_key):
                        tds.extract.connection.cols.delete(lower_mapping_key)
                # Enforce the column
                tds.enforce_column(Column(**attrs), folder_name, remote_name)
            if action == 'add_folder':
                tds.folders_common.add(Folder(**attrs))
            if action == 'delete_folder':
                tds.folders_common.delete(attrs['name'])
            if action == 'update_connection':
                # Only update the attributes of the connection we specify.
                # There are some attributes of a connection we do not need to update,
                # but are provided in the existing connection.
                connection = tds.connection[attrs['class_name']]
                for attr, value in attrs.items():
                    setattr(connection, attr, value)
                tds.connection.update(connection)

    def execute(self, context):
        all_tasks = context['ti'].xcom_pull(task_ids=self.tasks_task_id)
        ts = get_tableau_server(self.tableau_conn_id)
        snowflake_conn = BaseHook.get_connection(self.snowflake_conn_id)
        snowflake_creds = {'username': snowflake_conn.login, 'password': snowflake_conn.password}
        errors = list()
        # Attempt to update each Datasource
        for dsid, tasks in all_tasks.items():
            dl_path = f'downloads/{dsid}/'
            datasource_name = tasks['datasource_name']
            project = tasks['project']
            # Skipping datasources if they cannot be downloaded / published without timing out
            if tasks.get('skip'):
                logging.info('(SKIP) %s: %s', tasks['skip'], datasource_name)
                continue
            # Skipping datasources if they have no tasks that need to be updated
            if not self.__has_tasks_to_do(tasks):
                logging.info('(SKIP) No changes required: %s', datasource_name)
                continue
            # Update the Datasource
            try:
                # Download
                os.makedirs(dl_path, exist_ok=True)
                logging.info('Downloading Datasource: %s / %s', project, datasource_name)
                ds_path = ts.download.datasource(dsid, file_dir=dl_path, include_extract=False)
                # Update
                logging.info('Updating Datasource: %s / %s', project, datasource_name)
                # Remove extract section if the Datasource download does not have Extract data
                datasource = Datasource(ds_path)
                datasource.empty_extract()
                self.__do_action(tasks, datasource, 'update_connection')
                self.__do_action(tasks, datasource, 'modify_metadata')
                self.__do_action(tasks, datasource, 'delete_metadata')
                self.__do_action(tasks, datasource, 'add_metadata')
                self.__do_action(tasks, datasource, 'add_folder')
                self.__do_action(tasks, datasource, 'add_column')
                self.__do_action(tasks, datasource, 'modify_column')
                self.__do_action(tasks, datasource, 'delete_folder')
                logging.info('Saving datasource changes: %s / %s', project, datasource_name)
                datasource.save()
                # Publish
                logging.info('Publishing Datasource: %s / %s -> %s', project, datasource_name, dsid)
                ts.publish.datasource(ds_path, dsid, connection=snowflake_creds)
                logging.info('Published Successfully: %s / %s -> %s', project, datasource_name, dsid)
                os.remove(ds_path)
            except Exception as e:
                # Log the error, but wait to fail the task until all Datasources have been attempted
                logging.error(e)
                errors.append(e)
            finally:
                # Clean up downloaded and extracted files
                shutil.rmtree(dl_path, ignore_errors=True)
        # Fail task if there were errors updating any Datasources
        if errors:
            refresh_datasources(all_tasks, self.tableau_conn_id)
            raise Exception(f'Some datasources had errors when updating.\n{errors}')
