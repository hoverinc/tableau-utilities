import argparse
import os
import re
import yaml
import xmltodict
import shutil
import tableauserverclient as tsc
from zipfile import ZipFile
from collections import OrderedDict


ITEM_TYPES_YML = os.path.join(os.path.dirname(__file__), 'item_types.yml')


class TableauUtilitiesError(Exception):
    """ A minimum viable exception. """

    def __init__(self, message):
        self.message = message


def extract_tds(tdsx_path):
    """ Extracts the tds info to an OrderedDict from the tdsx.

    Args:
        tdsx_path (str): Path to the tdsx file
    """
    if tdsx_path:
        tdsx_dir = os.path.dirname(tdsx_path)
        tds_filename = os.path.basename(tdsx_path).replace('.tdsx', '.tds')
        tds_file_path = os.path.join(tdsx_dir, tds_filename)
        zip_file = ZipFile(tdsx_path)
        for z in zip_file.filelist:
            if z.filename.endswith('.tds'):
                z.filename = tds_filename
                zip_file.extract(member=z, path=tdsx_dir)
                break
        zip_file.close()
        with open(tds_file_path) as tds_file:
            tds = xmltodict.parse(tds_file.read())
        os.remove(tds_file_path)
        return tds
    else:
        return None


def update_tdsx(tdsx_path, tds):
    """ Updates the tdsx with the provided tds OrderedDict

    Args:
        tdsx_path (str): Path to the tdsx
        tds (obj): A OrderedDict of a tds file
    """
    if tdsx_path and tds:
        original_tdsx_path = os.path.abspath(tdsx_path)
        tdsx_file = os.path.basename(original_tdsx_path)
        # Move tdsx into a temporary folder while updating
        temp_tdsx_folder = os.path.join(os.path.dirname(original_tdsx_path),
                                        f'TEMP {tdsx_file.replace(".tdsx", "")}')
        os.makedirs(temp_tdsx_folder, exist_ok=False)
        temp_tdsx_path = os.path.join(temp_tdsx_folder, tdsx_file)
        shutil.move(original_tdsx_path, temp_tdsx_path)
        # Unzip the tdsx files
        extracted_files = list()
        tdsx_dir = os.path.dirname(temp_tdsx_path)
        with ZipFile(temp_tdsx_path) as z:
            for f in z.filelist:
                path = z.extract(member=f, path=tdsx_dir)
                extracted_files.append(path)
                if f.filename.endswith('.tds'):
                    tds_dict = path
        # Update tds file
        with open(tds_dict, 'w') as tds_file:
            tds_file.write(xmltodict.unparse(tds, pretty=True))
        # Repack the tdsx
        with ZipFile(temp_tdsx_path, 'w') as z:
            for file in extracted_files:
                arcname = file.split(tdsx_dir)[-1]
                z.write(file, arcname=arcname)
        # Move tdsx back to the original folder and remove any unpacked content
        shutil.move(temp_tdsx_path, original_tdsx_path)
        shutil.rmtree(temp_tdsx_folder)
    else:
        return None


class TableauServer:
    """ Connects to Tableau Online.
        Allows functionality to download, publish, refresh,
        and list information for sources and workbooks.

    Args:
        user (str): Tableau user name
        password (str): Tableau password
        site (str): The Tableau site name
            i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>
        url (str): The URL to Tableau Online
            i.e. https://<server_address>.online.tableau.com
        api_version (float): The Tableau API version
    """
    def __init__(self, **kwargs):
        user = kwargs.pop('user', None)
        password = kwargs.pop('password', None)
        site = kwargs.pop('site', None)
        url = kwargs.pop('url', None)
        api_version = kwargs.pop('api_version', 3.9)
        # Connect to Tableau Online when class is initialized
        self.server = self.__set_server(user, password, site, url, api_version)

    @staticmethod
    def __set_server(user, password, site, url, api_version):
        """ Logs in to Tableau Online and sets the server object """
        tableau_auth = tsc.TableauAuth(user, password, site)
        server = tsc.Server(url, api_version)
        server.auth.sign_in(tableau_auth)
        return server

    def list_objects(self, object_type, project=None, print_info=True):
        """ Fetch the name, project, and ID of the objects.

        Args:
            object_type: The type of objects to list, i.e. workbooks or datasources
            project: The name of the project to list objects for
            print_info: True to print the objects info; name, project, and ID

        Return: A table of information about all published objects
        """
        id_by_project_and_name = dict()
        objs = [o for o in tsc.Pager(self.server.__getattribute__(object_type))
                if o.project_name == project or not project]

        for o in objs:
            obj_id = o.id
            obj_name = o.name
            project_name = o.project_name
            if print_info:
                try:
                    print(f'{obj_id} : {obj_name} : {project_name}')
                except UnicodeEncodeError as err:
                    print(err)
            id_by_project_and_name[(project_name, obj_name)] = obj_id
        return id_by_project_and_name

    def list_datasources(self, project=None, print_info=True):
        """ Fetch the name, project, and ID of the datasources.

        :param project: The name of the project to list datasources for
        :param print_info: True to print the datasource info; name, project, and ID
        :return: A table of information about all published datasources
        """
        return self.list_objects('datasources', project, print_info)

    def list_workbooks(self, project=None, print_info=True):
        """ Fetch the name, project, and ID of the workbooks.

        :param project: The name of the project to list workbooks for
        :param print_info: True to print the workbooks info; name, project, and ID
        :return: A table of information about all published datasources
        """
        return self.list_objects('workbooks', project, print_info)

    def get_datasource_id(self, name, project):
        """ Get the id of the datasource by name and project

        :param name: The name of the datasource
        :param project: The name of the project the datasource is in
        :return: The datasource ID
        """
        tbl = self.list_datasources(project=project, print_info=False)
        return tbl.get((project, name))

    def get_workbook_id(self, name, project):
        """ Get the id of the workbook by name and project

        :param name: The name of the workbook
        :param project: The name of the project the workbook is in
        :return: The workbook ID
        """
        tbl = self.list_workbooks(project=project, print_info=False)
        return tbl.get((project, name))

    def download_datasource(self, dsid=None, name=None, project=None, filepath=None, include_extract=False):
        """ Download a datasource from Tableau Online.
            Provide either the datasource id, or the name and project of the source.

        :param dsid: The id of the datasource
        :param name: The name of the datasource
        :param project: The project the datasource is published to
        :param filepath: The path to output the download to
        :param include_extract: True to include the extract for the datasource
        :return: The path to the downloaded source
        """
        if not dsid:
            dsid = self.get_datasource_id(name, project)
        return self.server.datasources.download(dsid, filepath=filepath, include_extract=include_extract)

    def download_workbook(self, wbid=None, name=None, project=None, filepath=None, include_extract=False):
        """ Download a workbook from Tableau Online.
            Provide either the workbook id, or the name and project of the workbook.

        :param wbid: The id of the workbook
        :param name: The name of the workbook
        :param project: The project the workbook is published to
        :param filepath: The path to output the download to
        :param include_extract: True to include the extract for the workbook
        :return: The path to the downloaded workbook
        """
        if not wbid:
            wbid = self.get_workbook_id(name, project)
        return self.server.workbooks.download(wbid, filepath=filepath, include_extract=include_extract)

    def publish_datasource(self, tdsx_path, dsid=None, name=None, project=None, keep_tdsx=True):
        """ Publish a datasource from the local tdsx file.
            Provide either the datasource id, or the name and project of the source.

        :param tdsx_path: The path to the tdsx file
        :param dsid: The id of the datasource
        :param name: The name of the datasource
        :param project: The project the datasource is published to
        :param keep_tdsx: False to delete the tdsx file after publishing
        :return: None
        """
        if not dsid:
            dsid = self.get_datasource_id(name, project)

        datasource_item = self.server.datasources.get_by_id(dsid)
        self.server.datasources.publish(datasource_item, tdsx_path, 'Overwrite')
        if not keep_tdsx:
            os.remove(tdsx_path)

    def refresh_datasource(self, dsid=None, name=None, project=None):
        """ Refresh a datasource extract in Tableau Online.
            Provide either the datasource id, or the name and project of the source.

        :param dsid: The id of the datasource
        :param name: The name of the datasource
        :param project: The project the datasource is published to
        :return: None
        """
        if not dsid:
            dsid = self.get_datasource_id(name, project)

        datasource_item = self.server.datasources.get_by_id(dsid)
        try:
            self.server.datasources.refresh(datasource_item)
        except tsc.server.endpoint.exceptions.InternalServerError as e:
            raise TableauUtilitiesError(f'{e}\nDatasource extract already refreshing')

    def embed_credentials(self, dsid, credentials, connection_type):
        """ Embed the given credentials for all connections of a datasource of the given connection type.
            Only embeds Username and Password credentials.

        :param dsid: The ID of the datasource
        :param connection_type: Type of conncetion you want to embed creds for, i.e. snowflake
        :param credentials: The credentials dict to embed, i.e. {'username': 'user', 'password': 'password'}
        return: None
        """
        required_creds = ['username', 'password']
        missing_required_creds = [i for i in required_creds if i not in credentials or not credentials[i]]
        if missing_required_creds:
            raise TableauUtilitiesError(f'Missing required credentials: {", ".join(missing_required_creds)}')

        datasource_item = self.server.datasources.get_by_id(dsid)
        self.server.datasources.populate_connections(datasource_item)

        for c in datasource_item.connections:
            if c.connection_type.lower() == connection_type.lower():
                c.username = credentials['username']
                c.password = credentials['password']
                c.embed_password = True
                self.server.datasources.update_connection(datasource_item, c)


class TDS:
    """ Add, Update, Delete, Get, or List columns, folders, or connections from the XML of a .tds file.

        item_types:
            - column: The column element in the XML, used to alias and format a field in Tableau
            - folder: The folder element in the XML, used to organize columns in Tableau
            - connection: The connection element in the XML, used to connect to a source for data in Tableau
    """

    def __init__(self, tds):
        """
        Args:
            tds (obj): The Tableau datasource TDS object
        """
        ''' Required '''
        self.tds = tds

        ''' Situational '''
        self.item_type = None
        self.remote_name = None
        self.column_name = None
        self.folder_name = None
        self.role = None
        self.caption = None
        self.datatype = None
        self.role_type = None
        self.desc = None
        self.calculation = None
        self.conn_type = None
        self.conn_db = None
        self.conn_schema = None
        self.conn_host = None
        self.conn_role = None
        self.conn_user = None
        self.conn_warehouse = None

    def __apply_and_validate_kwargs(self, **kwargs):
        """ Applies and validates attributes passed

        Args:
            tds (obj): The Tableau datasource TDS object
            remote_name (str): The SQL alias for a column
            column_name (str): The underlying Tableau name for a column (usually will match the remote_name)
            folder_name (str):
            role (str):
            caption (str): The Tableau alias for a column; What is displayed in the workbook
            datatype (str): The underlying Tableau datatype
            role_type (str): The underlying role type in Tableau
            desc (str): The description of the column
            calculation (str): The Tableau calculation of the column
            conn_type (str): The connection type, e.g. 'snowflake'
            conn_db (str): The connection database
            conn_schema (str): The connection schema
            conn_host (str): The connection host URL
            conn_role (str): The connection role
            conn_warehouse (str): The connection warehouse
            conn_user (str): The connection user name
        """
        bad_attrib = dict()
        for key, value in kwargs.items():
            if key not in self.__dict__:
                bad_attrib[key] = 'Invalid Argument'
            else:
                self.__setattr__(key, value)

            if key == 'column_name':
                self.column_name = f'[{self.column_name}]' if self.column_name else None
            if key == 'role':
                self.role = f'{self.role}s' if self.role and self.item_type == 'folder' else self.role

        if self.item_type not in ['column', 'folder', 'connection', 'datasource-metadata']:
            bad_attrib['item_type'] = self.item_type
        if not self.tds:
            bad_attrib['tds'] = 'missing'
        if bad_attrib:
            raise TableauUtilitiesError(f'Invalid attribute(s): {bad_attrib}')

    def __identify_item(self, section, required_attribs):
        """ Returns the item if the item is found,
            otherwise nothing happens.

        Args:
            section (list): The section list containing the item
            required_attribs (dict): A dict of item attributes that identify the item
                e.g. {'column_name': 'the_name_of_the_column'}

        Returns: Item object
        """
        if not section:
            return None

        for item in section:
            # If any required attributes do not match,
            # This is not the item to be returned
            is_item = True
            for name, attribute in required_attribs.items():
                if item[name] != attribute:
                    is_item = False
            if is_item:
                return item

    @staticmethod
    def __get_section_paths(item_type):
        """ Gets the possible paths of XML element tags,
            leading to a section of the specified type of item.

        Args:
            item_type (str): The type of item to base the path off

        Returns: A list of element tags that leads to the section of the item
        """
        with open(ITEM_TYPES_YML) as file:
            cfg = yaml.safe_load(file)
            return cfg.get(item_type, {}).get('sections', [[]])

    def __determine_section_path(self, item_type):
        """ Determines the section path based on the item_type and structure of the XML.

        Args:
            item_type (str): The type of item
        """
        paths = self.__get_section_paths(item_type)

        if 'document-format-change-manifest' in self.tds['datasource']:
            if item_type == 'folder':
                return paths[1]

        return paths[0]

    def __determine_location(self, item_type):
        """ Determines the section location in the XML based on the item_type and structure of the XML.

        Args:
            item_type (str): The type of item
        """
        with open(ITEM_TYPES_YML) as file:
            cfg = yaml.safe_load(file)
            locations = cfg.get(item_type, {}).get('locations', [])

        if 'document-format-change-manifest' in self.tds['datasource']:
            if item_type == 'folder':
                return locations[1]

        return locations[0]

    def __determine_attributes(self, item_type):
        """ Determines the attributes of an item based on the item_type and structure of the XML.

        Args:
            item_type (str): The type of item
        """
        with open(ITEM_TYPES_YML) as file:
            cfg = yaml.safe_load(file)
            attributes = cfg.get(item_type, {}).get('attributes', [])

        if 'document-format-change-manifest' in self.tds['datasource']:
            if item_type == 'folder':
                return attributes[1]

        return attributes[0]

    def __add_section_and_item(self, tds, path, item, location):
        """ Adds the item and section to the tds

        Args:
            tds (obj): The tds OrderedDict object
            path (list): A list of keys in the order that leads the section in the tds
            item (obj): The item to add to the section
            location (dict): A dict of where an element of the path needs to be positioned in the tds,
                i.e. {'element': 'folders-common', 'before': 'extract'}
        """
        if location and path[0] == location['element'] and location['element'] not in tds:
            temp = OrderedDict()
            keys = list(tds.keys())
            insert_position = -1
            # Adjust the position in the XML based on a datasource position dict
            insert_position = keys.index(location['before']) if 'before' in location else keys.index(
                location['after']) + 1
            keys.insert(insert_position, path[0])
            for k in keys:
                if k == path[0]:
                    if len(path) > 1:
                        temp[k] = OrderedDict({path[1]: item})
                    else:
                        temp[k] = item
                else:
                    temp[k] = tds[k]
            # Reorder the dict and add the section & item
            for k, v in temp.items():
                if k in tds:
                    del tds[k]
                tds[k] = v

        if len(path) > 1:
            if path[0] in tds:
                self.__add_section_and_item(tds[path[0]], path[1:], item, location)
            else:
                tds[path[0]] = {path[1]: {}}
                self.__add_section_and_item(tds[path[0]], path[1:], item, location)
        elif len(path) == 1:
            tds[path[0]] = item

    @staticmethod
    def __delete_folder_item(folder, folder_item):
        """ Deletes a folder-item to a folder

        Args:
            folder (obj): An OrderedDict of a folder
            folder_item (obj): An OrderedDict of a folder-item
         """
        if 'folder-item' in folder and isinstance(folder['folder-item'], list):
            if folder_item in folder['folder-item']:
                folder['folder-item'].remove(folder_item)
        elif 'folder-item' in folder and folder_item == folder['folder-item']:
            del folder['folder-item']

    @staticmethod
    def __add_folder_item(folder, folder_item):
        """ Adds a folder-item to a folder after removing it from existing columns

        Args:
            folder (obj): An OrderedDict of a folder
            folder_item (obj): An OrderedDict of a folder-item
        """
        folder_items = folder.get('folder-item')
        if not folder_items:
            folder['folder-item'] = folder_item
        elif isinstance(folder_items, list):
            folder_items.append(folder_item)
        else:
            folder['folder-item'] = [folder['folder-item'], folder_item]

    def __update_column_folder(self):
        """ Adds a folder-item to a folder """
        folder_item = OrderedDict({'@name': self.column_name, '@type': 'field'})
        section = self.__get_section('folder')
        if not section:
            raise TableauUtilitiesError('Folder does not exist')
        folder_found = False
        for folder in section:
            # If the folder-item is already in a folder, remove it from that folder
            self.__delete_folder_item(folder, folder_item)
            # Then add the folder-item to the folder it should be in
            if folder['@name'] == self.folder_name and (
                    '@role' not in folder or folder['@role'][:-1] == self.role):
                self.__add_folder_item(folder, folder_item)
                folder_found = True
        if not folder_found:
            raise TableauUtilitiesError('Folder does not exist')

    def __update_metadata(self):
        """ Update the metadata for a column """
        if not self.column_name or not self.remote_name:
            raise TableauUtilitiesError('Missing local or remote name')
        for item_type in ['datasource-metadata', 'extract-metadata']:
            section = self.__get_section(item_type)
            if not section:
                raise TableauUtilitiesError('Metadata does not exist')
            metadata = self.__get_item(item_type, section)
            if metadata:
                # Set metadata local name to new local name
                old_local_name = metadata['local-name']
                new_local_name = self.column_name
                metadata['local-name'] = new_local_name

                # Delete old Column if the local name changed
                self.column_name = old_local_name
                col_section = self.__get_section('column')
                item = self.__get_item('column', col_section)
                if item and old_local_name != new_local_name:
                    col_section.remove(item)
                self.column_name = new_local_name

                # Replace all references of the old local name in calcs
                self.__recursively_re_sub(pattern='\\' + old_local_name,
                                          replacement=new_local_name,
                                          ordered_dict=col_section)

                # Set the mapping for the metadata and column
                self.__set_cols_mapping(item_type, old_local_name, new_local_name, table=metadata["parent-name"])

            else:
                raise TableauUtilitiesError('Metadata does not exist')

    def __set_cols_mapping(self, item_type, old_local_name, new_local_name, table):
        """ Set the mapping for a column

        Args:
             item_type (str): The type of metadata, either datasource-metadata or extract-metadata
             table (str): The table that the column comes from, e.g. '[Extract]'
        """
        section = self.__get_section(f'{item_type}-cols')
        item = OrderedDict({'@key': new_local_name,
                            '@value': f'{table}.[{self.remote_name}]'})

        if not section:
            self.__add_section_and_item(
                self.tds,
                path=self.__determine_section_path(f'{item_type}-cols'),
                item=item,
                location=self.__determine_location(f'{item_type}-cols')
            )
        else:
            col_found = False
            for mapping in section:
                if mapping['@key'] == old_local_name:
                    mapping['@key'] = new_local_name
                    col_found = True

            if not col_found:
                section.append(item)

    def __recursively_re_sub(self, pattern, replacement, ordered_dict):
        """ Recursively scans all strings in the OrderedDict
            to re.sub a specified pattern with a replacement.

        Args:
            pattern (str): The pattern to identify
            replacement (str): The replacement to substitute
            ordered_dict (obj): An OrderedDict object to recursively parse through
        """
        if isinstance(ordered_dict, OrderedDict):
            for k, v in ordered_dict.items():
                if isinstance(v, str):
                    ordered_dict[k] = re.sub(pattern, replacement, v)
                else:
                    self.__recursively_re_sub(pattern, replacement, ordered_dict[k])
        elif isinstance(ordered_dict, list):
            for item in ordered_dict:
                self.__recursively_re_sub(pattern, replacement, item)

    def __apply_item_attributes(self, item, attributes):
        """ Updates the item in the tds with the new item provided.

        Args:
            attributes (dict): A dict of attributes
                i.e. {name: {attribute: attribute_name, identifier: boolean}}
        """

        if not item and not isinstance(item, dict):
            raise TableauUtilitiesError(f'Item does not exist')

        for name, attribute in attributes.items():
            if self.item_type == 'column' and name == 'desc':
                if self.desc:
                    item[name] = OrderedDict({'formatted-text': {'run': self.desc}})
                elif name in item:
                    del item[name]
            elif self.item_type == 'column' and name == 'calculation':
                if self.calculation:
                    item[name] = OrderedDict({'@class': 'tableau', '@formula': self.calculation})
                elif name in item:
                    del item[name]
            elif self.item_type == 'column' and name == 'folder' and self.folder_name:
                self.__update_column_folder()
            elif self.item_type == 'column' and name == 'remote_name':
                if self.remote_name:
                    self.__update_metadata()
            elif self.item_type == 'datasource-metadata':
                self.__update_metadata()
            elif self.item_type == 'connection':
                if name == '@server':
                    item['@caption'] = self.__getattribute__(attribute)
                if name == '@schema':
                    # Update all references to the old schema to the new schema
                    # [old_schema].[table] -> [new_schema].[table]
                    self.__recursively_re_sub(pattern=r'(^\[)(' + item['connection'][name] + r')(].\[.+?)',
                                              replacement=r'\1' + self.__getattribute__(attribute) + r'\3',
                                              ordered_dict=self.tds)

                item['connection'][name] = self.__getattribute__(attribute)
            else:
                item[name] = self.__getattribute__(attribute)

    def __navigate_dict(self, dict_item, path):
        """ Navigates through a dictionary, based on the provided path,
            and returns the value at the end of the path.

        Args:
            dict_item (dict): A dictionary to navigate
            path (list): The path of navigation, i.e. ["datasource", "column"]

        Return: The final value of the dict from the path
        """
        if isinstance(dict_item, dict) and len(path) > 1 and path[0] in dict_item:
            return self.__navigate_dict(dict_item[path[0]], path[1:])
        elif isinstance(dict_item, dict) and len(path) == 1 and path[0] in dict_item:
            if isinstance(dict_item[path[0]], list):
                return dict_item[path[0]]
            else:
                dict_item[path[0]] = [dict_item[path[0]]]
                return dict_item[path[0]]
        elif not path:
            raise Exception(f'No Path given')
        else:
            raise KeyError(f'{path[0]} not in {dict_item}')

    def __get_section(self, item_type):
        """ Gets the section from the tds for the item_type and/or section_paths provided.
            If there are multiple paths given, it will return the first successful section.

        Args:
            item_type (str): The item type that correlates to this section
            section_paths (list): The paths to the section, i.e. [["datasource", "column"]]

        Returns: A section list from the tds
        """
        section_paths = self.__get_section_paths(item_type)
        for path in section_paths:
            try:
                return self.__navigate_dict(self.tds, path)
            except KeyError:
                pass
        return None

    def __get_item(self, item_type, section):
        """ Gets the item based on the identifier for the item_type provided,
            within the provided section.

        Args:
            item_type (str): The type of item to get
            section (list): The section of the tds that should contain the item

        Returns: Item object from the section
        """
        with open(ITEM_TYPES_YML) as file:
            cfg = yaml.safe_load(file)
            identifiers = cfg.get(item_type, {}).get('identifiers', [])
            identifiers = [
                {name: self.__getattribute__(attr) for name, attr in identifier.items()}
                for identifier in identifiers
            ]
        # identifiers = []
        # if item_type == 'folder':
        #     identifiers = [
        #         {"@name": self.folder_name, "@role": self.role},
        #         {"@name": self.folder_name}
        #     ]
        # if item_type == 'column':
        #     identifiers = [{"@name": self.column_name}]
        # if item_type in ['datasource-metadata', 'datasource-metadata-cols', 'extract-metadata', 'extract-metadata-cols']:
        #     identifiers = [{"remote-name": self.remote_name}]
        if item_type == 'connection':
            for item in section:
                if item['connection']['@class'] == self.conn_type:
                    return item
            # Only used for error message
            # identifiers = [{"connection": {'@class': self.conn_type}}]

        for identifier in identifiers:
            try:
                return self.__identify_item(section, identifier)
            except KeyError:
                pass
        raise TableauUtilitiesError(
            f'Item could not be identified. Identifiers do not exist:'
            f'\nIdentifiers: {identifiers}'
            f'\n\nSection: {section}'
        )

    def add(self, item_type, **kwargs):
        """ Adds the item to the tds, based on the attributes provided

        Args:
            item_type (str): 'column', 'folder', 'connection', or 'datasource-metadata'
            remote_name (str): The SQL alias for a column
            column_name (str): The underlying Tableau name for a column (usually will match the remote_name)
            folder_name (str): The Tableau folder that columns can be stored in
            role (str): 'measure' or 'dimension'
            caption (str): The Tableau alias for a column; What is displayed in the workbook
            datatype (str): The underlying Tableau datatype
            role_type (str): The underlying role type in Tableau
            desc (str): The description of the column
            calculation (str): The Tableau calculation of the column
            conn_type (str): The connection type, e.g. 'snowflake'
            conn_db (str): The connection database
            conn_schema (str): The connection schema
            conn_host (str): The connection host URL
            conn_role (str): The connection role
            conn_warehouse (str): The connection warehouse
            conn_user (str): The connection user name
        """
        self.__apply_and_validate_kwargs(item_type=item_type, **kwargs)
        section = self.__get_section(item_type)
        attributes = self.__determine_attributes(item_type)
        item = OrderedDict()
        self.__apply_item_attributes(item, attributes)
        if not section:
            path = self.__determine_section_path(item_type)
            location = self.__determine_location(item_type)
            self.__add_section_and_item(self.tds, path, item, location)
        elif self.__get_item(item_type, section):
            raise TableauUtilitiesError('Item already exists')
        else:
            section.append(item)

    def list(self, item_type):
        """ Lists items from the tds, based on the attributes provided

        Args:
            item_type (str): 'column', 'folder', 'connection', or 'datasource-metadata'
        """
        self.__apply_and_validate_kwargs(item_type=item_type)
        return self.__get_section(item_type)

    def get(self, item_type, **kwargs):
        """ Gets the item from the tds, based on the attributes provided

        Args:
            item_type (str): 'column', 'folder', 'connection', or 'datasource-metadata'
            remote_name (str): The SQL alias for a column
            column_name (str): The underlying Tableau name for a column (usually will match the remote_name)
            folder_name (str): The Tableau folder that columns can be stored in
            role (str): 'measure' or 'dimension'
            conn_type (str): The connection type, e.g. 'snowflake'
        """
        self.__apply_and_validate_kwargs(item_type=item_type, **kwargs)
        section = self.__get_section(item_type)
        return self.__get_item(self.item_type, section)

    def delete(self, item_type, **kwargs):
        """ Deletes the item from the tds, based on the attributes provided

        Args:
            item_type (str): 'column', 'folder', 'connection', or 'datasource-metadata'
            remote_name (str): The SQL alias for a column
            column_name (str): The underlying Tableau name for a column (usually will match the remote_name)
            folder_name (str): The Tableau folder that columns can be stored in
            role (str): 'measure' or 'dimension'
            conn_type (str): The connection type, e.g. 'snowflake'
        """
        self.__apply_and_validate_kwargs(item_type=item_type, **kwargs)
        section = self.__get_section(item_type)
        item = self.__get_item(self.item_type, section)
        section.remove(item)

    def update(self, item_type, **kwargs):
        """ Updates the item from the tds, based on the attributes provided

        Args:
            item_type (str): 'column', 'folder', 'connection', or 'datasource-metadata'
            remote_name (str): The SQL alias for a column
            column_name (str): The underlying Tableau name for a column (usually will match the remote_name)
            folder_name (str): The Tableau folder that columns can be stored in
            role (str): 'measure' or 'dimension'
            caption (str): The Tableau alias for a column; What is displayed in the workbook
            datatype (str): The underlying Tableau datatype
            role_type (str): The underlying role type in Tableau
            desc (str): The description of the column
            calculation (str): The Tableau calculation of the column
            conn_type (str): The connection type, e.g. 'snowflake'
            conn_db (str): The connection database
            conn_schema (str): The connection schema
            conn_host (str): The connection host URL
            conn_role (str): The connection role
            conn_warehouse (str): The connection warehouse
            conn_user (str): The connection user name
        """
        self.__apply_and_validate_kwargs(item_type=item_type, **kwargs)
        section = self.__get_section(item_type)
        item = self.__get_item(self.item_type, section)
        attributes = self.__determine_attributes(item_type)
        self.__apply_item_attributes(item, attributes)


def do_args(argv=None):
    """ Parse arguments.

    Args:
        argv (list): an array arguments ['--server', 'myserver',...]

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(
        description='Manipulate columns in a Tableau datasource')
    parser.add_argument(
        '--server',
        help='Tableau Server address. i.e. <server_address> in https://<server_address>.online.tableau.com',
        default=None
    )
    parser.add_argument(
        '--site',
        help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
        default=None
    )
    parser.add_argument(
        '--api_version',
        help='Tableau API version',
        default='2.8'
    )
    parser.add_argument('--user', default=None, help='Tableau username')
    parser.add_argument('--password', default=None, help='Tableau password')
    parser.add_argument(
        '--conn_user',
        help='Username for embed credentials. See --embed_creds.',
        default=False
    )
    parser.add_argument(
        '--conn_pw',
        help='Password for embed credentials. See --embed_creds',
        default=False
    )
    parser.add_argument(
        '--conn_type',
        help='Connection type for embed credentials. See --embed_creds',
        default=False
    )
    parser.add_argument('--conn_db', default=None, help='Connection Database')
    parser.add_argument('--conn_schema', default=None, help='Connection Schema')
    parser.add_argument('--conn_host', default=None, help='Connection Host (URL)')
    parser.add_argument('--conn_role', default=None, help='Connection Role')
    parser.add_argument('--conn_warehouse', default=None, help='Connection Warehouse')
    parser.add_argument(
        '--list_datasources',
        action='store_true',
        default=False,
        help='Print the datasource names and ids'
    )
    parser.add_argument(
        '--list_workbooks',
        action='store_true',
        default=False,
        help='Print the workbook names and ids'
    )
    parser.add_argument(
        '--update_connection',
        action='store_true',
        default=False,
        help='Update the connection information in the datasource'
    )
    parser.add_argument(
        '--embed_creds',
        action='store_true',
        default=False,
        help='Embed credentials for the datasource'
             ' indicated by datasource ID (--dsid) if supplied, or by --dsn and --project.'
             ' Provide username (--cred_user), password (--cred_pw), and connection type (--conn_type) to embed.'
    )
    parser.add_argument(
        '--id',
        default=None,
        help='The ID of the datasource or workbook. See --list_datasources'
    )
    parser.add_argument('--name', default=None, help='The datasource or workbook name')
    parser.add_argument(
        '--project',
        default=None,
        help='The project name for the datasource or workbook'
    )
    parser.add_argument(
        '--download_ds',
        default=False,
        action='store_true',
        help='Download the datasource indicated by datasource ID (--id) if supplied, or by --name and --project.'
    )
    parser.add_argument(
        '--download_wb',
        default=False,
        action='store_true',
        help='Download the workbook indicated by workbook ID (--id) if supplied, or by --name and --project.'
    )
    parser.add_argument(
        '--refresh',
        default=False,
        action='store_true',
        help='Refresh the datasource indicated by datasource ID (--dsid) if supplied, or by --dsn and --project.'
    )
    parser.add_argument(
        '--tdsx',
        default=None,
        help='Path to the tdsx file. See --modify_column and --publish'
    )
    parser.add_argument(
        '--publish',
        default=False,
        action='store_true',
        help='Publish a datasource. Supply filename in --tdsx and ID in --id'
    )
    parser.add_argument(
        '--add_column',
        default=False,
        action='store_true',
        help='Add the column identified by --column_name in the downloaded datasource. See --tdsx'
    )
    parser.add_argument(
        '--modify_column',
        default=False,
        action='store_true',
        help='Change the column identified by --column_name in the downloaded datasource. See --tdsx'
    )
    parser.add_argument(
        '--add_folder',
        default=False,
        action='store_true',
        help='Add the folder identified by --folder_name in the downloaded datasource. See --tdsx'
    )
    parser.add_argument(
        '--delete_folder',
        default=False,
        action='store_true',
        help='Delete the folder identified by --folder_name in the downloaded datasource. See --tdsx'
    )
    parser.add_argument(
        '--column_name',
        help='The name of the column. See --add_column, and --modify_column'
    )
    parser.add_argument(
        '--remote_name',
        help='The remote (SQL) name of the column. See --add_column, and --modify_column'
    )
    parser.add_argument('--caption', help='Short name/Alias for the column')
    parser.add_argument(
        '--title_case_caption',
        default=False,
        action='store_true',
        help='Converts caption to title case. Applied after --caption'
    )
    parser.add_argument('--role', default=None, choices=['measure', 'dimension'])
    parser.add_argument('--desc', help='A Tableau column description')
    parser.add_argument('--calculation', help='A Tableau calculation')
    parser.add_argument(
        '--datatype',
        default=None,
        choices=['date', 'datetime', 'integer', 'real', 'string']
    )
    parser.add_argument(
        '--role_type',
        default=None,
        choices=['nominal', 'ordinal', 'quantitative']
    )
    parser.add_argument(
        '--folder_name',
        default=None,
        help='The name of the folder. See --add_column, --modify_column, --add_folder and --delete_folder'
    )
    if argv:
        return parser.parse_args(argv)
    else:
        return parser.parse_args()


def main():
    args = do_args()
    tdsx = args.tdsx

    needs_tableau_server = (
        args.list_datasources
        or args.list_workbooks
        or args.download_ds
        or args.download_wb
        or args.publish
        or args.embed_creds
        or args.refresh
    )

    if needs_tableau_server:
        missing_creds = dict()
        for _arg in ['user', 'password', 'site', 'server']:
            if not args.__getattribute__(_arg):
                missing_creds.setdefault('missing', [])
                missing_creds['missing'].append(f'--{_arg}')
        if missing_creds:
            raise TableauUtilitiesError(missing_creds)
        ts = TableauServer(
            user=args.user,
            password=args.password,
            site=args.site,
            url=f'https://{args.server}.online.tableau.com'
        )
    if args.list_datasources:
        ts.list_datasources(project=args.project, print_info=True)
    if args.list_workbooks:
        ts.list_workbooks(project=args.project, print_info=True)
    if args.download_ds:
        tdsx = ts.download_datasource(args.id, name=args.name, project=args.project, include_extract=False)
        print(f'Downloaded to {tdsx}')
    tds_dict = extract_tds(tdsx)
    tds = TDS(tds_dict)
    if args.download_wb:
        ts.download_workbook(wbid=args.id, name=args.name, project=args.project,
                             include_extract=False)
    if args.add_column:
        tds.add(
            item_type='column', column_name=args.column_name,
            remote_name=args.remote_name, caption=args.caption, folder_name=args.folder_name,
            role=args.role, role_type=args.role_type, datatype=args.datatype, desc=args.desc,
            calculation=args.calculation
        )
    if args.add_folder:
        tds.add(item_type='folder', folder_name=args.folder_name, role=args.role)
    if args.delete_folder:
        tds.delete(item_type='folder', folder_name=args.folder_name, role=args.role)
    if args.modify_column:
        tds.update(
            item_type='column', column_name=args.column_name,
            remote_name=args.remote_name, caption=args.caption, folder_name=args.folder_name,
            role=args.role, role_type=args.role_type, datatype=args.datatype, desc=args.desc,
            calculation=args.calculation
        )
    if args.update_connection:
        tds.update(
            item_type='connection', conn_user=args.conn_user, conn_type=args.conn_type,
            conn_role=args.conn_role, conn_db=args.conn_db, conn_schema=args.conn_schema,
            conn_host=args.conn_host, conn_warehouse=args.conn_warehouse
        )
    update_tdsx(tdsx, tds_dict)
    if args.publish:
        ts.publish_datasource(tdsx, dsid=args.id, name=args.name, project=args.project)
    if args.embed_creds:
        creds = {'username': args.conn_user, 'password': args.conn_pw}
        ts.embed_credentials(dsid=args.id, credentials=creds, connection_type=args.conn_type)
    if args.refresh:
        ts.refresh_datasource(dsid=args.id, name=args.name, project=args.project)
        print(f'Refreshed {args.id}')


if __name__ == '__main__':
    main()
