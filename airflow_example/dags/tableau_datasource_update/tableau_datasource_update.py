"""
  Updates each Tableau datasource's columns/connection/etc, according to the config files.
"""

import datetime
import time
import logging
import json
import os
import shutil
import re
import ast
from copy import deepcopy
from tdscc import TDS, TableauServer, extract_tds, update_tdsx, TDSCCError

from airflow import DAG, models
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook


CFG_PATH = 'dags/tableau_datasource_update/configs'
with open(os.path.join(CFG_PATH, 'column_persona_config.json')) as read_config:
    PERSONA_CFG = json.load(read_config)
with open(os.path.join(CFG_PATH, 'datasource_project_config.json')) as read_config:
    DS_PROJECT_CFG = json.load(read_config)
with open(os.path.join(CFG_PATH, 'column_config.json')) as read_config:
    COLUMN_CFG = json.load(read_config)
with open(os.path.join(CFG_PATH, 'tableau_calc_config.json')) as read_config:
    CALC_CFG = json.load(read_config)


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


def refresh_datasource(tasks, tableau_conn_id='tableau_default', snowflake_conn_id='snowflake_default'):
    """ Refresh a datasource extract.

    Args:
        tasks: A dictionary with the columns to add or modify.
        tableau_conn_id (str): The Tableau connection ID
        snowflake_conn_id (str): The connection ID for Snowflake.
    """

    if isinstance(tasks, str):
        tasks = ast.literal_eval(tasks)

    conn = BaseHook.get_connection(tableau_conn_id)
    ts = TableauServer(user=conn.login, password=conn.password, url=conn.host,
                       site=conn.extra_dejson['site'])

    snowflake_conn = BaseHook.get_connection(snowflake_conn_id)
    embeded_credentials_attempts = 0
    embed_tries = 10
    while embeded_credentials_attempts < embed_tries:
        try:
            ts.embed_credentials(tasks["dsid"], connection_type='snowflake',
                                 credentials={'username': snowflake_conn.login,
                                              'password': snowflake_conn.password})
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
    if tasks['datasource_name'] in no_refresh:
        logging.info('No refresh required - skipping refresh of %s %s',
                     tasks["dsid"], tasks['datasource_name'])
        return None

    try:
        ts.refresh_datasource(tasks["dsid"])
        logging.info('Refreshed %s %s', tasks["dsid"], tasks['datasource_name'])
    except Exception as error:
        if isinstance(error, TDSCCError) or 'Not queuing a duplicate.' in str(error):
            logging.info(error)
            logging.info('Skipping Refresh %s %s ... Already running',
                         tasks["dsid"], tasks['datasource_name'])
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
        super().__init__(*args, **kwargs)
        self.snowflake_conn_id = kwargs.get('snowflake_conn_id', 'snowflake_default')
        self.tableau_conn_id = kwargs.get('tableau_conn_id', 'tableau_default')
        self.actions = ['add_column', 'modify_column', 'add_folder', 'delete_folder', 'update_connection']
        self.tasks = {a: [] for a in self.actions}
        self.tasks['datasource_name'] = kwargs.get('datasource_name')
        self.tasks['project'] = kwargs.get('project')
        self.column_cfg = kwargs.get('column_cfg', {}).get(self.tasks['datasource_name'])
        self.persona_cfg = kwargs.get('persona_cfg')
        self.tds = None

    def __set_connection_attributes(self):
        """ Sets attributes of the datasource connection. """
        snowflake_conn = BaseHook.get_connection(self.snowflake_conn_id)

        # In the event that we switch to a different data warehouse,
        # we will need to update the host specification here
        if snowflake_conn.conn_type.lower() != 'snowflake':
            raise Exception('Connection must be of type: Snowflake')

        return {
            'conn_type': 'snowflake',
            'conn_db': snowflake_conn.extra_dejson['database'],
            'conn_schema': snowflake_conn.extra_dejson['schema'],
            'conn_host': f'{snowflake_conn.host}.snowflakecomputing.com',
            'conn_role': snowflake_conn.extra_dejson['role'],
            'conn_user': snowflake_conn.login,
            'conn_warehouse': snowflake_conn.extra_dejson['warehouse']
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

    def __is_column_in_folder(self, col_attributes):
        """ Determine if the column is in the given folder.

        Args:
            col_attributes: Dict of column attributes.

        Returns: True, False, or None if the folder doesn't exist.
        """

        folder = TDS(tds=self.td).get('folder', **col_attributes)
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

    def __add_task(self, action, attributes):
        """ Add a task to the dictionary of tasks:
            add_column, modify_column, add_folder, or delete_folder

            Sample:
                {
                    "dsid": "abc123def456",
                    "datasource_name": "Datasource Name Here",
                    "project": "General",
                    "add_column": [attrib, attrib],
                    "modify_column": [attrib, attrib]
                    "add_folder": [attrib, attrib]
                    "delete_folder": [attrib, attrib]
                    "update_connection": [attrib, attrib]
                }
        Args:
            action: The name of action to do.
            attributes: Dict of attributes for the action to use.
        """
        if action and action not in self.actions:
            raise Exception(f'Invalid action {action}')

        if action:
            self.tasks[action].append(attributes)
            logging.info('Adding to task table action: %s dsid: %s Datasource Name: %s Attributes: %s',
                         action, self.tasks['dsid'], self.tasks['datasource_name'], attributes)

    def __append_folders_from_tds(self, folders_from_tds):
        """ Appends the folders_from_tds with folders found in the datasource

        :param folders_from_tds: The table to append information to
        :return: None
        """
        if self.tasks['dsid'] not in folders_from_tds:
            folders = TDS(tds=self.tds).list('folder')
            if isinstance(folders, list):
                folders = [{'name': f['@name'],
                            'role': f['@role'][:-1] if '@role' in f else None}
                           for f in folders]
            elif folders:
                folders = [{'name': folders['@name'],
                            'role': folders['@role'][:-1] if '@role' in folders else None}]
            else:
                folders = []
            folders_from_tds[self.tasks['dsid']] = {'name': self.tasks['datasource_name'],
                                                    'folders': folders}

    def __append_folders_from_config(self, col_attributes, folders_from_cfg):
        """ Appends the folders_from_cfg with folders found in the config

        Args:
            col_attributes: Dict of attributes about the column
            folders_from_cfg: The table to append information to
        """
        folder_info = {'name': col_attributes['folder_name'], 'role': col_attributes['role']}
        if self.tasks['dsid'] not in folders_from_cfg:
            folders_from_cfg[self.tasks['dsid']] = {'name': self.tasks['datasource_name'],
                                                    'folders': [folder_info]}
        elif folder_info not in folders_from_cfg[self.tasks['dsid']]['folders']:
            folders_from_cfg[self.tasks['dsid']]['folders'].append(folder_info)

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
    def __different_connection(tds_conn, credentials):
        """ Compare the connection from the tds to attributes we expect.

        Args:
            tds_conn (dict): The OrderedDict connection from the tds.
            credentials (dict): The credentials that should be attributed to the connection

        Returns: True if there is a difference between the connection in the source and the credentials provided
        """
        diff = False
        if not tds_conn or not credentials:
            return diff

        if '@class' in tds_conn and tds_conn['@class'] != credentials.get('conn_type'):
            diff = True
        if '@dbname' in tds_conn and tds_conn['@dbname'] != credentials.get('conn_db'):
            diff = True
        if '@schema' in tds_conn and tds_conn['@schema'] != credentials.get('conn_schema'):
            diff = True
        if '@server' in tds_conn and tds_conn['@server'] != credentials.get('conn_host'):
            diff = True
        if '@service' in tds_conn and tds_conn['@service'] != credentials.get('conn_role'):
            diff = True
        if '@username' in tds_conn and tds_conn['@username'] != credentials.get('conn_user'):
            diff = True
        if '@warehouse' in tds_conn and tds_conn['@warehouse'] != credentials.get('conn_warehouse'):
            diff = True
        return diff

    def __add_folder_task_exists(self, col_attributes):
        """ Checks if the add_folder task has already been added as a task for the datasource.

        Args:
            col_attributes (dict): A dict of attributes about the column

        Returns: True if the task exists
        """
        try:
            add_folder_task_folder_role_exists = [
                a for a in self.tasks['add_folder']
                if a['folder_name'] == col_attributes['folder_name'] and a['role'] == col_attributes['role']]
            return add_folder_task_folder_role_exists != []
        except KeyError:
            return False

    def __compare_folders(self, folders_from_tds, folders_from_cfg):
        """ Compares folders found in the datasource and in the config.
            If there are folders in the source that are not in the config,
            a task will be added to delete the folder.

        Args:
            folders_from_tds (dict): The table of folders from the datasource's tds
            folders_from_cfg (dict): The table of folders from the config
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
                        action='delete_folder',
                        attributes={'folder_name': tds_f['name'], 'role': tds_f['role']}
                    )

    def execute(self, context):
        """ Update Tableau datasource according to config. """
        conn = BaseHook.get_connection(self.tableau_conn_id)
        ts = TableauServer(user=conn.login, password=conn.password, url=conn.host,
                           site=conn.extra_dejson['site'])

        dsid_tbl = ts.list_datasources(print_info=False)
        self.tasks['dsid'] = dsid_tbl.get((self.tasks['project'], self.tasks['datasource_name']))

        folders_from_tds = {}
        folders_from_cfg = {}

        if self.tasks['dsid'] is None:
            logging.error('No datasource %s in %s.', self.tasks['datasource_name'], self.tasks['project'])
            return None

        dl_path = f"downloads/{self.tasks['dsid']}/"
        os.makedirs(dl_path, exist_ok=True)
        tdsx = ts.download_datasource(self.tasks['dsid'], filepath=dl_path, include_extract=False)
        self.tds = extract_tds(tdsx)
        # Clean up downloaded and extracted files
        shutil.rmtree(dl_path, ignore_errors=True)

        # Add connection task
        conn_attribs = self.__set_connection_attributes()
        tds_conn = TDS(tds=self.tds).list('connection')
        if isinstance(tds_conn, list):
            tds_conn = tds_conn[0]['connection']
        else:
            tds_conn = tds_conn['connection']
        if self.__different_connection(tds_conn, conn_attribs):
            self.__add_task(action='update_connection', attributes=conn_attribs)
        else:
            logging.info('No changes needed for connection in %s', self.tasks['datasource_name'])

        # Add Column and Folder tasks
        for caption, col_info in self.column_cfg.items():
            # Replace full column names with their local-name in the calculation
            if 'calculation' in col_info and col_info['calculation']:
                captions = set(re.findall(r'\[.+?\]', col_info['calculation']))
                for full_name in captions:
                    key = re.sub(r'[\[\]]+', '', full_name)
                    if key in self.column_cfg:
                        col_info['calculation'] = col_info['calculation'].replace(
                            full_name, f"[{self.column_cfg[key]['local-name']}]")
            else:
                col_info['calculation'] = None

            column_attribs = self.__set_column_attributes(
                caption, col_info, self.persona_cfg[col_info['persona']])

            column_from_tds = TDS(tds=self.tds).get('column', **column_attribs)

            metadata_diff = False
            if column_attribs['remote_name']:
                metadata = TDS(tds=self.tds).get('datasource-metadata', **column_attribs)
                metadata_local_name = re.sub(r'^\[|]$', '', metadata['local-name']) if metadata else None
                if metadata_local_name and metadata_local_name != column_attribs['column_name']:
                    metadata_diff = True

            if column_from_tds:
                column_from_tds = dict(column_from_tds)
                column_from_tds['@name'] = re.sub(r'^\[|]$', '', column_from_tds['@name'])
            folder_check = self.__is_column_in_folder(column_attribs)
            different_column = self.__different_column(column_from_tds, column_attribs)
            # If the folder is missing and there is not already a task to add this folder/role,
            # then add the task
            if folder_check is None and not self.__add_folder_task_exists(column_attribs):
                self.__add_task(action='add_folder', attributes=column_attribs)
            if not column_from_tds:
                self.__add_task(action='add_column', attributes=column_attribs)
            elif different_column or not folder_check or metadata_diff:
                self.__add_task(action='modify_column', attributes=column_attribs)
            else:
                logging.info('No changes needed for %s in %s', caption, self.tasks['datasource_name'])

            # Get the table of folders from the datasource
            self.__append_folders_from_tds(folders_from_tds)

            # Get the table of folders from the config
            self.__append_folders_from_config(column_attribs, folders_from_cfg)

        # Add tasks to delete folders that are not in the config for each datasource
        self.__compare_folders(folders_from_tds, folders_from_cfg)

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
        super().__init__(*args, **kwargs)
        self.tasks_task_id = kwargs.get('tasks_task_id')
        self.tableau_conn_id = kwargs.get('tableau_conn_id', 'tableau_default')
        self.snowflake_conn_id = kwargs.get('snowflake_conn_id', 'snowflake_default')
        # Set on execute
        self.tasks = None
        self.tds = None

    def __has_any_task(self):
        """ Check if there are any tasks to be done """
        for attributes in self.tasks.values():
            if isinstance(attributes, list) and attributes:
                return True
        return False

    def __do_action(self, tdsx, action_name, action, item_type):
        """ Executes all of the action items

        Args:
            tdsx (str): The path to the TDSX file
            action_name (str): The name of the action to be done
            action (str): The action to be done
            item_type (str): The type of item
        """
        for item in self.tasks[action_name]:
            try:
                logging.info('Going to %s %s: %s -- %s', item_type, tdsx, item)
                TDS(tds=self.tds).__getattribute__(action)(item_type, **item)
            except TDSCCError as err:
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
                    TDS(tds=self.tds).__getattribute__(action)(item_type, **temp_item)

    def execute(self, context):
        self.tasks = context['ti'].xcom_pull(task_ids=self.tasks_task_id)

        conn = BaseHook.get_connection(self.tableau_conn_id)
        ts = TableauServer(user=conn.login, password=conn.password,
                           url=conn.host, site=conn.extra_dejson['site'])

        dl_path = f'downloads/{self.tasks["dsid"]}/'
        try:
            # Skip updating these datasources until we optimize them well enough
            # to downloaded & published without timing out
            excluded = ast.literal_eval(models.Variable.get('EXCLUDED_DATASOURCES'))
            if self.tasks['datasource_name'] in excluded:
                logging.info('Marked as excluded - Skipping Updating Datasource: %s',
                             self.tasks['datasource_name'])
                return None

            if self.__has_any_task():
                # Download
                os.makedirs(dl_path, exist_ok=True)
                logging.info('Downloading Datasource: %s -> %s',
                             self.tasks['project'], self.tasks['datasource_name'])
                tdsx = ts.download_datasource(dsid=self.tasks['dsid'], filepath=dl_path, include_extract=True)
                # Update
                logging.info('Extracting tds from %s -> %s: %s',
                             self.tasks['project'], self.tasks['datasource_name'], tdsx)
                self.tds = extract_tds(tdsx)
                self.__do_action(tdsx, 'update_connection', 'update', 'connection')
                self.__do_action(tdsx, 'add_folder', 'add', 'folder')
                self.__do_action(tdsx, 'add_column', 'add', 'column')
                self.__do_action(tdsx, 'modify_column', 'update', 'column')
                self.__do_action(tdsx, 'delete_folder', 'delete', 'folder')
                logging.info('Updating tdsx %s %s',
                             self.tasks["dsid"], self.tasks['datasource_name'])
                update_tdsx(tdsx_path=tdsx, tds=self.tds)
                # Publish
                logging.info('About to publish %s %s %s',
                             self.tasks["dsid"], self.tasks['datasource_name'], tdsx)
                ts.publish_datasource(tdsx, self.tasks["dsid"], keep_tdsx=False)
                logging.info('Published %s %s', self.tasks["dsid"], self.tasks['datasource_name'])
            else:
                logging.info('No tasks to update - Skipping Updating Datasource: %s',
                             self.tasks['datasource_name'])

        except Exception as e:
            refresh_datasource(self.tasks, self.tableau_conn_id, self.snowflake_conn_id)
            raise Exception(e)
        finally:
            # Clean up downloaded and extracted files
            shutil.rmtree(dl_path, ignore_errors=True)

        return None


default_dag_args = {
    'start_date': datetime.datetime(2020, 8, 1),
    'retries': 2
}

dag = DAG(
    dag_id='update_tableau_datasources',
    schedule_interval='@hourly',
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=360),
    max_active_runs=1,
    default_args=default_dag_args)

for ds, project in DS_PROJECT_CFG.items():
    task_name = re.sub(r'[^a-zA-Z0-9]+', '_', ds).lower()
    add_tasks = TableauDatasourceTasks(
        dag=dag,
        task_id=f'add_tasks_{task_name}',
        snowflake_conn_id='snowflake_tableau_datasource',
        tableau_conn_id='tableau_default',
        datasource_name=ds,
        project=project,
        column_cfg=COLUMN_CFG,
        persona_cfg=PERSONA_CFG
    )

    update = TableauDatasourceUpdate(
        dag=dag,
        task_id=f'update_{task_name}',
        snowflake_conn_id='snowflake_tableau_datasource',
        tableau_conn_id='tableau_default',
        tasks_task_id=f'add_tasks_{task_name}'
    )

    refresh = PythonOperator(
        dag=dag,
        task_id=f'refresh_{task_name}',
        python_callable=refresh_datasource,
        op_kwargs={'snowflake_conn_id': 'snowflake_tableau_datasource',
                   'tableau_conn_id': 'tableau_default',
                   'tasks': "{{task_instance.xcom_pull(task_ids='%s')}}" %
                            f'add_tasks_{task_name}'}
    )

    add_tasks >> update >> refresh
