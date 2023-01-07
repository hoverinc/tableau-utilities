"""
Updates each Tableau datasource's columns/connection/etc, according to the config files.
"""

import datetime
import time
import logging
import yaml
import os
import shutil
import re
import ast
from copy import deepcopy
from tableau_utilities import TableauServer, TDS, extract_tds, update_tdsx, TableauUtilitiesError

from airflow import DAG, models
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


CFG_PATH = 'dags/tableau_datasource_update/configs'
with open(os.path.join(CFG_PATH, 'column_persona_config.yaml')) as read_config:
    PERSONA_CFG = yaml.safe_load(read_config)
with open(os.path.join(CFG_PATH, 'datasource_project_config.yaml')) as read_config:
    DS_PROJECT_CFG = yaml.safe_load(read_config)
with open(os.path.join(CFG_PATH, 'column_config.yaml')) as read_config:
    COLUMN_CFG = yaml.safe_load(read_config)
with open(os.path.join(CFG_PATH, 'tableau_calc_config.yaml')) as read_config:
    CALC_CFG = yaml.safe_load(read_config)


def invert_config(iterator, config):
    """ Helper function to invert the column config and calc config.
        Output -> {datasource: {column: info}}
    Args:
        iterator (dict): The iterator to append invert data to.
        config (dict): The config to invert.
    """
    for column, i in config.items():
        for datasource in i['datasources']:
            new_info = deepcopy(i)
            del new_info['datasources']
            new_info['local-name'] = datasource['local-name']
            new_info['remote_name'] = datasource['sql_alias'] if 'sql_alias' in datasource else None
            iterator.setdefault(datasource['name'], {column: new_info})
            iterator[datasource['name']].setdefault(column, new_info)


# Setup the column_cfg for the rest of the DAG
temp = dict()
invert_config(temp, COLUMN_CFG)
invert_config(temp, CALC_CFG)
COLUMN_CFG = deepcopy(temp)
del temp


def refresh_datasource(tasks, tableau_conn_id='tableau_default', snowflake_conn_id='gcp_snowflake_default'):
    """ Refresh a datasource extract.
    Args:
        tasks: A dictionary with the columns to add or modify.
        tableau_conn_id (str): The Tableau connection ID
        snowflake_conn_id (str): The connection ID for Snowflake.
    """

    if isinstance(tasks, str):
        tasks = ast.literal_eval(tasks)

    conn = BaseHook.get_connection(tableau_conn_id)
    ts = TableauServer(
        url=conn.host,
        token_name=conn.extra_dejson['token_name'],
        token_secret=conn.extra_dejson['token_secret'],
        site=conn.extra_dejson['site']
    )
    snowflake_conn = BaseHook.get_connection(snowflake_conn_id)

    for datasource_id, ds_tasks in tasks.items():
        embeded_credentials_attempts = 0
        embed_tries = 10
        while embeded_credentials_attempts < embed_tries:
            try:
                ts.embed_credentials(
                    datasource_id,
                    connection_type='snowflake',
                    credentials={'username': snowflake_conn.login, 'password': snowflake_conn.password}
                )
                logging.info('Successfully embedded credentials')
                embeded_credentials_attempts = embed_tries
            except AttributeError as err:
                if embeded_credentials_attempts < embed_tries - 1:
                    embeded_credentials_attempts += 1
                    time.sleep(10)
                    logging.warning('Embedding credentials failed: %s', err)
                    logging.info('Retrying embedding credentials: %s / %s attempts',
                                 embeded_credentials_attempts, embed_tries)
                else:
                    raise Exception(err) from err

        # All listed datasources in this variable won't be refreshed
        # Common use-case for not refreshing a datasource, is because it has a live connection
        no_refresh = ast.literal_eval(models.Variable.get('NO_REFRESH_DATASOURCES'))
        if ds_tasks['datasource_name'] in no_refresh:
            logging.info('No refresh required - skipping refresh of %s %s',
                         datasource_id, ds_tasks['datasource_name'])
            return None

        try:
            ts.refresh_datasource(datasource_id)
            logging.info('Refreshed %s %s', ds_tasks, ds_tasks['datasource_name'])
        except Exception as error:
            if isinstance(error, TableauUtilitiesError) or 'Not queuing a duplicate.' in str(error):
                logging.info(error)
                logging.info('Skipping Refresh %s %s ... Already running',
                             datasource_id, ds_tasks['datasource_name'])
            else:
                raise Exception(error) from error


class TableauDatasourceTasks(models.BaseOperator):
    """ Compares config files to the published datasource,
        to get a dictionary of tasks needing to be updated.
    Args:
        snowflake_conn_id (str): The Snowflake connection ID, used for the datasource connection info
        tableau_conn_id (str): The Tableau connection ID
        datasource_name (str): The name of the datasource
        project (str): The project of the datasource in Tableau Online
        column_cfg (dict): The config information for the datasource.
    Returns: A dict of tasks to be updated for the datasource.
    """
    def __init__(self, *args, **kwargs):
        self.snowflake_conn_id = kwargs.pop('snowflake_conn_id', 'gcp_snowflake_default')
        self.tableau_conn_id = kwargs.pop('tableau_conn_id', None)
        self.actions = ['add_column', 'modify_column', 'add_folder', 'delete_folder', 'update_connection']
        self.datasource_project_cfg = kwargs.pop('datasource_project_cfg', {})
        self.column_cfg = kwargs.pop('column_cfg', {})
        self.persona_cfg = kwargs.pop('persona_cfg')
        super().__init__(*args, **kwargs)
        # Set on execution
        self.tasks = dict()

    def __set_connection_attributes(self):
        """ Sets attributes of the datasource connection. """
        snowflake_hook = SnowflakeHook(self.snowflake_conn_id)

        return {
            'conn_type': 'snowflake',
            'conn_db': snowflake_hook.database,
            'conn_schema': snowflake_hook.schema,
            'conn_host': f'{snowflake_hook.account}.snowflakecomputing.com',
            'conn_role': snowflake_hook.role,
            'conn_user': snowflake_hook.user,
            'conn_warehouse': snowflake_hook.warehouse
        }

    @staticmethod
    def __set_column_attributes(caption, col_config, persona_cfg):
        """ Sets attributes as a function of caption and cfg.
        :param caption: The top-level key of the cfg.
        :param col_config: A dictionary of the column info from the config file
        :param persona_cfg: A dictionary of the persona config file
        """
        return {
            'caption': caption,
            'column_name': col_config['local-name'],
            'role': persona_cfg['role'],
            'role_type': persona_cfg['role_type'],
            'datatype': persona_cfg['datatype'],
            'desc': col_config['description'],
            'calculation': col_config['calculation'],
            'folder_name': col_config['folder'],
            'remote_name': col_config['remote_name']
        }

    def __is_column_in_folder(self, tds, col_attributes):
        """ Determine if the column is in the given folder.
        Args:
            col_attributes: Dict of column attributes.
        Returns: True, False, or None if the folder doesn't exist.
        """

        folder = TDS(tds).get('folder', **col_attributes)
        if not folder:
            return None
        if folder.get('folder-item') is None:
            return False
        folder_items = folder.get('folder-item')
        if isinstance(folder_items, list):
            for folder_item in folder_items:
                if folder_item['@name'] == f'[{col_attributes["column_name"]}]':
                    return True
        else:
            if folder_items['@name'] == f'[{col_attributes["column_name"]}]':
                return True
        return False

    def __add_task(self, datasource_id, action, attributes, other=None):
        """ Add a task to the dictionary of tasks:
            add_column, modify_column, add_folder, or delete_folder
            Sample:
                {
                    "dsid": "abc123def456",
                    "datasource_name": "Datasource Name",
                    "project": "Project Name",
                    "add_column": [attrib, attrib],
                    "modify_column": [attrib, attrib]
                    "add_folder": [attrib, attrib]
                    "delete_folder": [attrib, attrib]
                    "update_connection": [attrib, attrib]
                }
        Args:
            datasource_id (str): The ID of the datasource
            action: The name of action to do.
            attributes: Dict of attributes for the action to use.
        """
        if action and action not in self.actions:
            raise Exception(f'Invalid action {action}')

        if action:
            self.tasks[datasource_id][action].append(attributes)
            logging.info(
                'Adding to task table action: %s dsid: %s Datasource Name: %s\nAttributes:\n\t%s\n\t%s',
                action, datasource_id, self.tasks[datasource_id]['datasource_name'], attributes, other
            )

    def __append_folders_from_tds(self, tds, datasource_id, folders_from_tds):
        """ Appends the folders_from_tds with folders found in the datasource
        Args:
            datasource_id (str): The ID of the datasource
            folders_from_tds (dict): The table to append information to
        Returns: None
        """
        if datasource_id not in folders_from_tds:
            folders = TDS(tds).list('folder')
            if isinstance(folders, list):
                folders = [{'name': f['@name'],
                            'role': f['@role'][:-1] if '@role' in f else None}
                           for f in folders]
            elif folders:
                folders = [{'name': folders['@name'],
                            'role': folders['@role'][:-1] if '@role' in folders else None}]
            else:
                folders = []
            folders_from_tds[datasource_id] = {
                'name': self.tasks[datasource_id]['datasource_name'],
                'folders': folders
            }

    def __append_folders_from_config(self, datasource_id, col_attributes, folders_from_cfg):
        """ Appends the folders_from_cfg with folders found in the config
        Args:
            datasource_id (str): The ID of the datasource
            col_attributes: Dict of attributes about the column
            folders_from_cfg: The table to append information to
        """
        folder_info = {'name': col_attributes['folder_name'], 'role': col_attributes['role']}
        if datasource_id not in folders_from_cfg:
            folders_from_cfg[datasource_id] = {
                'name': self.tasks[datasource_id]['datasource_name'],
                'folders': [folder_info]
            }
        elif folder_info not in folders_from_cfg[datasource_id]['folders']:
            folders_from_cfg[datasource_id]['folders'].append(folder_info)

    @staticmethod
    def __different_column(tds_col, attributes):
        """ Compare the column from the tds to attributes we expect.
        Args:
            tds_col: The OrderedDict column from the tds.
            attributes: The column attributes generated from the config file entry.
        Returns: bool
        """
        diff = False
        # If there is no column - either in the config or the source - then return false
        if not tds_col or not attributes:
            return diff

        if '@caption' in tds_col and tds_col['@caption'] != attributes.get('caption'):
            diff = True
        if 'desc' in tds_col and tds_col['desc']['formatted-text']['run'] != attributes.get('desc'):
            diff = True
        if 'calculation' in tds_col and tds_col['calculation']['@formula'] != attributes.get('calculation'):
            diff = True
        if '@role' in tds_col and tds_col['@role'] != attributes.get('role'):
            diff = True
        if '@type' in tds_col and tds_col['@type'] != attributes.get('role_type'):
            diff = True
        if '@datatype' in tds_col and tds_col['@datatype'] != attributes.get('datatype'):
            diff = True
        return diff

    @staticmethod
    def __different_connection(tds_conn, attributes):
        """ Compare the connection from the tds to attributes we expect.
        Args:
            tds_conn: The OrderedDict connection from the tds.
            attributes: The column attributes generated from the config file entry.
        Returns: bool
        """
        diff = False
        if not tds_conn or not attributes:
            return diff

        map_attrs = {
            '@class': 'conn_type',
            '@dbname': 'conn_db',
            '@schema': 'conn_schema',
            '@server': 'conn_host',
            '@service': 'conn_role',
            '@username': 'conn_user',
            '@warehouse': 'conn_warehouse',
        }
        for tds_attr, cfg_attr in map_attrs.items():
            if tds_attr in tds_conn and tds_conn[tds_attr].lower() != attributes[cfg_attr].lower():
                diff = True
        return diff

    def __add_folder_task_exists(self, dsid, col_attributes):
        """ Checks if the add_folder task has already been added as a task for the datasource.
        Args:
            dsid (str): The ID of the datasource
            col_attributes (dict): The column attributes containing the folder_name and role
        Returns: A boolean; True if an add_folder task was added
        """
        try:
            add_folder_task_folder_role_exists = [
                a for a in self.tasks[dsid]['add_folder']
                if a['folder_name'] == col_attributes['folder_name'] and a['role'] == col_attributes['role']]
            return add_folder_task_folder_role_exists != []
        except KeyError:
            return False

    def __compare_folders(self, datasource_id, folders_from_tds, folders_from_cfg):
        """ Compares folders found in the datasource and in the config.
            If there are folders in the source that are not in the config,
            a task will be added to delete the folder.
        :param folders_from_tds: The table of folders from the datasource's tds
        :param folders_from_cfg: The table of folders from the config
        :return: None
        """

        def exists(tds_f, cfg_folders):
            for cfg_f in cfg_folders:
                if tds_f['name'] == cfg_f['name']:
                    if not tds_f['role'] or tds_f['role'] == cfg_f['role']:
                        return True
            return False

        for dsid, ds_info in folders_from_tds.items():
            for tds_f in ds_info['folders']:
                cfg_folders = folders_from_cfg[dsid]['folders']
                if not exists(tds_f, cfg_folders):
                    self.__add_task(
                        datasource_id=datasource_id,
                        action='delete_folder',
                        attributes={'folder_name': tds_f['name'], 'role': tds_f['role']},
                        other=tds_f,
                    )

    def execute(self, context):
        """ Update Tableau datasource according to config. """
        conn = BaseHook.get_connection(self.tableau_conn_id)
        ts = TableauServer(
            url=conn.host,
            token_name=conn.extra_dejson['token_name'],
            token_secret=conn.extra_dejson['token_secret'],
            site=conn.extra_dejson['site']
        )
        ds_tbl = ts.list_datasources(print_info=False)
        for datasource, project in self.datasource_project_cfg.items():
            dsid = ds_tbl.get((project, datasource))
            if not dsid:
                logging.error('COULD NOT FIND DATASOURCE: %s in %s', datasource, project)
                continue
            self.tasks[dsid] = {a: [] for a in self.actions}
            self.tasks[dsid]['project'] = project
            self.tasks[dsid]['datasource_name'] = datasource

            folders_from_tds = {}
            folders_from_cfg = {}
            dl_path = f"downloads/{dsid}/"
            os.makedirs(dl_path, exist_ok=True)
            tdsx = ts.download_datasource(dsid, filepath=dl_path, include_extract=False)
            tds = extract_tds(tdsx)
            # Clean up downloaded and extracted files
            shutil.rmtree(dl_path, ignore_errors=True)

            # Add connection task
            conn_attribs = self.__set_connection_attributes()
            tds_conn = TDS(tds).list('connection')
            if isinstance(tds_conn, list):
                tds_conn = tds_conn[0]['connection']
            else:
                tds_conn = tds_conn['connection']
            if self.__different_connection(tds_conn, conn_attribs):
                self.__add_task(dsid, action='update_connection', attributes=conn_attribs, other=tds_conn)
            else:
                logging.info('No changes needed for connection in %s', datasource)

            # Add Column and Folder tasks
            for caption, col_info in self.column_cfg[datasource].items():
                # Replace full column names with their local-name in the calculation
                if 'calculation' in col_info and col_info['calculation']:
                    captions = set(re.findall(r'\[.+?\]', col_info['calculation']))
                    for full_name in captions:
                        key = re.sub(r'[\[\]]+', '', full_name)
                        if key in self.column_cfg[datasource]:
                            col_info['calculation'] = col_info['calculation'].replace(
                                full_name, f"[{self.column_cfg[datasource][key]['local-name']}]")
                else:
                    col_info['calculation'] = None

                column_attribs = self.__set_column_attributes(
                    caption, col_info, self.persona_cfg[col_info['persona']])

                column_from_tds = TDS(tds).get('column', **column_attribs)

                metadata_diff = False
                if column_attribs['remote_name']:
                    metadata = TDS(tds).get(
                        'datasource-metadata', remote_name=column_attribs['remote_name']
                    )
                    metadata_local_name = re.sub(r'^\[|]$', '', metadata['local-name']) if metadata else None
                    if metadata_local_name and metadata_local_name != column_attribs['column_name']:
                        metadata_diff = True

                if column_from_tds:
                    column_from_tds = dict(column_from_tds)
                    column_from_tds['@name'] = re.sub(r'^\[|]$', '', column_from_tds['@name'])
                folder_check = self.__is_column_in_folder(tds, column_attribs)
                different_column = self.__different_column(column_from_tds, column_attribs)
                # If the folder is missing and there is not already a task to add this folder/role,
                # then add the task
                if folder_check is None and not self.__add_folder_task_exists(dsid, column_attribs):
                    self.__add_task(dsid, action='add_folder', attributes=column_attribs)
                if not column_from_tds:
                    self.__add_task(dsid, action='add_column', attributes=column_attribs)
                elif different_column or not folder_check or metadata_diff:
                    self.__add_task(
                        dsid,
                        action='modify_column',
                        attributes=column_attribs,
                        other={
                            'column': column_from_tds,
                            'in_folder': folder_check,
                            'metadata_diff': metadata_diff
                        }
                    )
                else:
                    logging.info('No changes needed for %s in %s', caption, datasource)

                # Get the table of folders from the datasource
                self.__append_folders_from_tds(tds, dsid, folders_from_tds)

                # Get the table of folders from the config
                self.__append_folders_from_config(dsid, column_attribs, folders_from_cfg)

            # Add tasks to delete folders that are not in the config for each datasource
            self.__compare_folders(dsid, folders_from_tds, folders_from_cfg)

        return self.tasks


class TableauDatasourceUpdate(models.BaseOperator):
    """ Downloads the datasource.
        Makes all necessary updates to the datasource.
        Publishes the datasource.
    Args:
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
    def __has_any_task(tasks):
        """ Check if there are any tasks to be done
        Args:
            tasks (dict): The tasks to be done
        """
        for attributes in tasks.values():
            if isinstance(attributes, list) and attributes:
                return True
        return False

    @staticmethod
    def __do_action(tasks, tdsx, tds, action_name, action, item_type):
        """ Executes the action, for each item to do for that action
        Args:
            tdsx (str): The path to the tdsx file
            tds (OrderedDict): The dict of generated from the datasource's XML
            action_name (str): The name of the action
            action (str): The TDS action to do
            item_type (str): The type of item to do the action for
        """
        for item in tasks[action_name]:
            try:
                logging.info('Going to %s %s: %s -- %s', action, item_type, tdsx, item)
                getattr(TDS(tds), action)(item_type, **item)
            except TableauUtilitiesError as err:
                # If a source is updated again before it has refreshed in tableau,
                # it will not detect the folders in the source, and try to add them all again
                if item_type == 'folder' and action == 'add':
                    logging.info('Skipping Adding Folder: %s -- Already Exists', item['folder_name'])

                # TEMP -- THIS SHOULD BE REMOVED EVENTUALLY
                # THIS IS HERE SO THAT COLUMNS THAT DO NOT EXIST WILL NOT ERROR ON MISSING METADATA
                elif item_type == 'column' and action == 'add' and 'Metadata does not exist' in str(err):
                    logging.warning('Missing SQL metadata for column: %s', item['remote_name'])
                    temp_item = deepcopy(item)
                    del temp_item['remote_name']
                    getattr(TDS(tds), action)(item_type, **temp_item)

    def execute(self, context):
        all_tasks = context['ti'].xcom_pull(task_ids=self.tasks_task_id)

        conn = BaseHook.get_connection(self.tableau_conn_id)
        ts = TableauServer(
            url=conn.host,
            token_name=conn.extra_dejson['token_name'],
            token_secret=conn.extra_dejson['token_secret'],
            site=conn.extra_dejson['site']
        )

        for dsid, tasks in all_tasks.items():
            dl_path = f'downloads/{dsid}/'
            try:
                # TEMP: Skip updating these datasources until we optimize them well enough
                # to downloaded & published without timing out
                excluded = ast.literal_eval(models.Variable.get('EXCLUDED_DATASOURCES'))
                if tasks['datasource_name'] in excluded:
                    logging.info('Marked as excluded - Skipping Updating Datasource: %s',
                                 tasks['datasource_name'])
                    return None

                if self.__has_any_task(tasks):
                    # Download
                    os.makedirs(dl_path, exist_ok=True)
                    logging.info('Downloading Datasource: %s -> %s',
                                 tasks['project'], tasks['datasource_name'])
                    tdsx = ts.download_datasource(dsid, filepath=dl_path, include_extract=True)
                    # Update
                    logging.info('Extracting tds from %s -> %s: %s',
                                 tasks['project'], tasks['datasource_name'], tdsx)
                    tds = extract_tds(tdsx)
                    self.__do_action(tasks, tdsx, tds, 'update_connection', 'update', 'connection')
                    self.__do_action(tasks, tdsx, tds, 'add_folder', 'add', 'folder')
                    self.__do_action(tasks, tdsx, tds, 'add_column', 'add', 'column')
                    self.__do_action(tasks, tdsx, tds, 'modify_column', 'update', 'column')
                    self.__do_action(tasks, tdsx, tds, 'delete_folder', 'delete', 'folder')
                    logging.info('Updating tdsx %s %s', dsid, tasks['datasource_name'])
                    update_tdsx(tdsx_path=tdsx, tds=tds)
                    # Publish
                    logging.info('About to publish %s %s %s',
                                 dsid, tasks['datasource_name'], tdsx)
                    ts.publish_datasource(tdsx, dsid)
                    logging.info('Published %s %s', dsid, tasks['datasource_name'])
                    os.remove(tdsx)
                else:
                    logging.info('No tasks to update - Skipping Updating Datasource: %s',
                                 tasks['datasource_name'])

            except Exception as e:
                refresh_datasource(all_tasks, self.tableau_conn_id, self.snowflake_conn_id)
                raise Exception(e)
            finally:
                # Clean up downloaded and extracted files
                shutil.rmtree(dl_path, ignore_errors=True)

        return None


default_dag_args = {
    'start_date': datetime.datetime(2023, 1, 1),
}

dag = DAG(
    dag_id='update_tableau_datasources',
    schedule_interval='@daily',
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
    max_active_runs=1,
    default_args=default_dag_args
)

add_tasks = TableauDatasourceTasks(
    dag=dag,
    task_id='gather_datasource_update_tasks',
    snowflake_conn_id='snowflake_tableau_datasource',
    tableau_conn_id='tableau_update_datasources',
    datasource_project_cfg=DS_PROJECT_CFG,
    column_cfg=COLUMN_CFG,
    persona_cfg=PERSONA_CFG
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
    python_callable=refresh_datasource,
    op_kwargs={'snowflake_conn_id': 'snowflake_tableau_datasource',
               'tableau_conn_id': 'tableau_update_datasources',
               'tasks': "{{task_instance.xcom_pull(task_ids='%s')}}" %
                        'gather_datasource_update_tasks'}
)

add_tasks >> update >> refresh
