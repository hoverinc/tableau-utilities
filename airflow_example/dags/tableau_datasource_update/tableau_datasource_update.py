"""
Updates each Tableau datasource's columns/connection/etc, according to the config files.
"""

import datetime
import logging
import os
import shutil
import ast
from tableau_utilities import Datasource, TableauServer
from tableau_utilities.tableau_file.tableau_file_objects import Folder, Column, MetadataRecord

import dags.tableau_datasource_update.configs.configuration as cfg
from airflow import DAG, models
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
# This is our custom SnowflakeHook - Your code will need to be adapted
from plugins.snowflake_connection.snowflake_operator_manual_update import SnowflakeHook

AIRFLOW_ENV = models.Variable.get('AIRFLOW_ENVIRONMENT', '').upper()
EXCLUDED_DATASOURCES = ast.literal_eval(models.Variable.get('EXCLUDED_DATASOURCES', '[]'))
SKIP_REFRESH = ast.literal_eval(models.Variable.get('NO_REFRESH_DATASOURCES', '[]'))
UPDATE_ACTIONS = [
    'delete_metadata',
    'modify_metadata',
    'add_metadata',
    'add_column',
    'modify_column',
    'add_folder',
    'delete_folder',
    'update_connection'
]


def get_tableau_server(tableau_conn_id: str):
    """ Returns a TableauServer object """
    conn = BaseHook.get_connection(tableau_conn_id)
    api_version = conn.extra_dejson.get('api_version')
    if api_version:
        api_version = float(api_version)
    return TableauServer(
        host=conn.host
        , site=conn.extra_dejson.get('site')
        , api_version=api_version
        , personal_access_token_name=conn.extra_dejson.get('personal_access_token_name')
        , personal_access_token_secret=conn.extra_dejson.get('personal_access_token_secret')
    )


def refresh_datasources(tasks, tableau_conn_id='tableau_default'):
    """ Refresh a datasource extract.

    Args:
        tasks (str|dict): A dictionary of the actions for updating the datasource.
        tableau_conn_id (str): The Tableau connection ID
    """
    if isinstance(tasks, str):
        tasks: dict = ast.literal_eval(tasks)
    ts = get_tableau_server(tableau_conn_id)

    for datasource_id in tasks:
        datasource_name = tasks[datasource_id]['datasource_name']
        # All listed datasources in this variable won't be refreshed
        # Common use-case for not refreshing a datasource, is because it has a live connection
        if datasource_name in SKIP_REFRESH:
            logging.info('(SKIP) Marked to skip refresh: %s %s', datasource_id, datasource_name)
            continue

        try:
            ts.refresh.datasource(datasource_id)
            logging.info('Refreshed: %s %s', datasource_id, datasource_name)
        except Exception as error:
            if 'Not queuing a duplicate.' in str(error):
                logging.info(error)
                logging.info('(SKIP) Refresh already running: %s %s',
                             datasource_id, datasource_name)
            else:
                raise Exception(error) from error


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

        # github_conn = BaseHook.get_connection(self.github_conn_id)
        # config = cfg.Config(
        #     githup_token=github_conn.password,
        #     repo_name=github_conn.extra_dejson.get('repo_name'),
        #     repo_branch=github_conn.extra_dejson.get('repo_branch'),
        #     subfolder=github_conn.extra_dejson.get('subfolder')
        # )
        #
        # ts = get_tableau_server(self.tableau_conn_id)
        # expected_conn_attrs = self.__set_connection_attributes()
        #
        # # Get the ID for each datasource in the config
        # for ds in ts.get.datasources():
        #     if ds not in config.datasources:
        #         continue
        #     config.datasources[ds].id = ds.id
        #
        # # for datasource in config.datasources:
        #     if not datasource.id:
        #         logging.error('!! Datasource not found in Tableau Online: %s / %s',
        #                       datasource.project_name, datasource.name)
        #         continue
        #     dsid = datasource.id
            # Set default dict attributes for tasks, for each datasource
            self.tasks[dsid] = {a: [] for a in UPDATE_ACTIONS}
            self.tasks[dsid]['project'] = datasource.project_name
            self.tasks[dsid]['datasource_name'] = datasource.name
            # if not config.in_maintenance_window and AIRFLOW_ENV not in ['STAGING', 'DEV']:
            #     self.tasks[dsid]['skip'] = 'Outside maintenance window'
            #     logging.info('(SKIP) Outside maintenance window: %s', datasource.name)
            #     continue
            # elif datasource.name in EXCLUDED_DATASOURCES:
            #     self.tasks[dsid]['skip'] = 'Marked to exclude'
            #     logging.info('(SKIP) Marked to exclude: %s', datasource.name)
            #     continue
            # logging.info('Checking Datasource: %s', datasource.name)
            # # Download the Datasource for comparison
            # dl_path = f"downloads/{dsid}/"
            # os.makedirs(dl_path, exist_ok=True)
            # ds_path = ts.download.datasource(dsid, file_dir=dl_path, include_extract=False)
            # tds = Datasource(ds_path)
            # # Cleanup downloaded file after assigning the Datasource
            # shutil.rmtree(dl_path, ignore_errors=True)
            # # Add connection task, if there is a difference
            # self.__compare_connection(dsid, datasource.name, tds.connection, expected_conn_attrs)
            # # Add folder tasks, if folders need to be added/deleted
            # self.__compare_folders(dsid, tds.folders_common, datasource.folders)
            # # Add Column tasks, if there are missing columns, or columns need to be updated
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


default_dag_args = {
    'start_date': datetime.datetime(2020, 8, 1)
}

dag = DAG(
    dag_id='update_tableau_datasources',
    schedule_interval='@hourly',
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
    max_active_runs=1,
    default_args=default_dag_args
)

with open('dags/tableau_datasource_update/tableau_datasource_update.md') as doc_md:
    dag.doc_md = doc_md.read()

add_tasks = TableauDatasourceTasks(
    dag=dag,
    task_id='gather_datasource_update_tasks',
    snowflake_conn_id='snowflake_tableau_datasource',
    tableau_conn_id='tableau_update_datasources',
    github_conn_id='github_dbt_repo'
)

update = TableauDatasourceUpdate(
    dag=dag,
    task_id='update_datasources',
    snowflake_conn_id='snowflake_tableau_datasource',
    tableau_conn_id='tableau_update_datasources',
    tasks_task_id='gather_datasource_update_tasks'
)

refresh = PythonOperator(
    dag=dag,
    task_id='refresh_datasources',
    python_callable=refresh_datasources,
    op_kwargs={
        'tableau_conn_id': 'tableau_update_datasources',
        'tasks': "{{task_instance.xcom_pull(task_ids='gather_datasource_update_tasks')}}"
    }
)

add_tasks >> update >> refresh
