import logging
import sys
import re
import xml.etree.ElementTree as ET
import xmltodict
from datetime import datetime
from dataclasses import dataclass, astuple
from typing import Literal
from tableau_utilities.general.funcs import transform_tableau_object

@dataclass
class TableauFileObject:
    """
        A Tableau File object is any element in a Tableau Online/Server object that can be downloaded,
        i.e. Columns, Folders, Connections, etc, in Datasources/Workbooks.

        This is the base class for a Tableau File object,
        with the minimum required attributes and functionality.
        Child classes will inherit the functionality provided by this base class.

        The data types of attributes from child class will be converted to the appropriate type,
        if it is provided as a string instead.
    """

    def __bool__(self):
        return len([v for k, v in self.__dict__.items() if v and k != 'tag']) > 0

    def __existing_str_attr(self, attr: str):
        """ Returns: True if the attribute exists and is a string """
        return (
            hasattr(self, attr)
            and getattr(self, attr)
            and isinstance(getattr(self, attr), str)
        )

    def __to_int(self, attr: str):
        """
            Set the attribute to an integer,
            if the class has the attribute,
            and if the attribute is a string.
        """
        if self.__existing_str_attr(attr):
            setattr(self, attr, int(getattr(self, attr)))

    def __to_bool(self, attr: str):
        """
            Set the attribute to a boolean,
            if the class has the attribute,
            and if the attribute is a string.
        """
        if self.__existing_str_attr(attr):
            setattr(self, attr, getattr(self, attr).lower() in ['true', 'yes'])

    def __to_datetime(self, attr: str):
        """
            Set the attribute to a datetime,
            if the class has the attribute,
            and if the attribute is a string.
        """
        if self.__existing_str_attr(attr):
            setattr(self, attr, datetime.strptime(getattr(self, attr), '%Y-%m-%d %H:%M:%S.%f'))

    def __post_init__(self):
        # Convert to Boolean
        self.__to_bool('contains_null')
        self.__to_bool('datatype_customized')
        self.__to_bool('extract_engine')
        self.__to_bool('enabled')
        self.__to_bool('hidden')
        self.__to_bool('incremental_updates')
        self.__to_bool('user_specific')
        # Convert to Integer
        self.__to_int('approx_count')
        self.__to_int('collation_flag')
        self.__to_int('count')
        self.__to_int('ordinal')
        self.__to_int('port')
        self.__to_int('precision')
        self.__to_int('scale')
        self.__to_int('width')
        # Convert to Datetime
        self.__to_datetime('timestamp_start')

    def dict(self):
        pass

    def xml(self):
        """ Returns the FileObject as an XML Element """
        return ET.fromstring(xmltodict.unparse({self.tag: self.dict()}, pretty=True))


class TableauFileObjects(list):
    """
        A list of FileObject items from a Tableau file,
        i.e. Columns, Folders, Connections, etc
    """
    def __init__(self, seq=(), item_class=TableauFileObject, tag=''):
        # Set and validate
        self._item_class = item_class
        self.tag = tag
        # Enforce listed items
        if isinstance(seq, (dict, TableauFileObject)):
            seq = [seq]
        super().__init__(self.__validate_item(item) for item in seq)

    def __getitem__(self, item):
        if isinstance(item, int):
            return super().__getitem__(item)
        return super().__getitem__(self.index(item))

    def __setitem__(self, item, newitem):
        if isinstance(item, int):
            super().__setitem__(item, newitem)
        else:
            super().__setitem__(self.index(item), newitem)

    def __eq__(self, other):
        same = True
        for item in self:
            if item not in other:
                same = False
        for item in other:
            if item not in self:
                same = False
        return same

    def __del__(self):
        self.clear()

    def __validate_item(self, item):
        if isinstance(item, dict):
            _item = transform_tableau_object(item)
            try:
                return self._item_class(**_item)
            except TypeError as err:
                print(item)
                print(_item)
                raise TypeError(err) from err
        elif not isinstance(item, self._item_class):
            raise TypeError(f'Item must be of type {self._item_class.__name__} or dict, not {item.__class__.__name__}')
        return item

    def _to_dict(self):
        """ Converts all items into dicts """
        for idx, item in enumerate(self):
            if isinstance(item, self._item_class):
                self[idx] = item.dict()

    def _to_obj(self):
        """ Converts all items into the appropriate Tableau FileObject """
        for idx, item in enumerate(self):
            if isinstance(item, dict):
                item = transform_tableau_object(item)
                self[idx] = self._item_class(**item)

    def add(self, item):
        """ Add the provided item to the list

        Args:
            item (dict|TableauFileObject): The item to be added
        """
        item = self.__validate_item(item)
        if item not in self:
            self.append(item)

    def extend(self, items):
        """ Extends the Datasource items with another list of Datasource items.

        Args:
            items (TableauFileObjects[TableauFileObject]|list[TableauFileObject]): A list of Tableau FileObjects
        """
        for item in items:
            self.add(item)

    def update(self, item):
        """ Add the provided item to the list

        Args:
            item (dict|TableauFileObject): The item to be added
        """
        item = self.__validate_item(item)
        self[item] = item

    def get(self, item):
        """ Get the item from the list

        Args:
            item (dict|str|TableauFileObject): The item to get

        Returns: The Tableau FileObject
        """
        if isinstance(item, int):
            return self[item]
        for _item in self:
            if _item == item:
                return _item

    def delete(self, item):
        """ Delete the item from the list

        Args:
            item (dict|str|TableauFileObject): The item to delete
        """
        self.remove(item)

    def pop(self, item):
        """ Delete and return the item from the list

        Args:
            item (dict|str|TableauFileObject|int): The item to delete

        Returns: The Tableau FileObject
        """
        if isinstance(item, int):
            return self.pop(item)
        else:
            return self.pop(self.index(item))

    def xml(self):
        """ Returns the TableauFileObjects as an XML Element """
        self._to_dict()
        xml = ET.fromstring(xmltodict.unparse({self.tag: self}, pretty=True))
        self._to_obj()
        return xml


@dataclass
class Column(TableauFileObject):
    """ The Column Tableau file object """
    name: str
    datatype: Literal['boolean', 'date', 'datetime', 'integer', 'real', 'string', 'table']
    role: Literal['dimension', 'measure']
    type: Literal['nominal', 'ordinal', 'quantitative']
    tag: str = 'column'
    default_role: str = None
    default_type: str = None
    default_format: str = None
    auto_column: str = None
    xmlns: str = None
    semantic_role: str = None
    caption: str = None
    desc: str = None
    calculation: str = None
    aggregation: str = None
    user_auto_column: str = None
    param_domain_type: str = None
    value: str = None
    alias: str = None
    ordinal: int = None
    datatype_customized: bool = None
    hidden: bool = None
    aliases: list = None
    members: dict = None
    range: dict = None
    fiscal_year_start: int = None
    visual_totals: str = None
    ns0_auto_column: str = None
    xmlns_ns0: str = None

    def __post_init__(self):
        if not re.match(r'^\[.+]$', self.name):
            self.name = f'[{self.name}]'
        if self.aliases and isinstance(self.aliases, dict):
            if isinstance(self.aliases['alias'], list):
                self.aliases = self.aliases['alias']
            else:
                self.aliases = [self.aliases['alias']]
        if self.desc and isinstance(self.desc, dict):
            self.desc = self.desc['formatted-text']['run']
        if self.calculation and isinstance(self.calculation, dict):
            self.calculation = self.calculation['@formula'] if self.calculation['@class'] == 'tableau' else None
        super().__post_init__()

    def __hash__(self):
        return hash(str(astuple(self)))

    def __eq__(self, other):
        name = ''
        if isinstance(other, str):
            name = other
        elif isinstance(other, dict):
            name = other.get('name')
        elif isinstance(other, (Column, object)):
            name = other.name

        if not re.match(r'^\[.+]$', name):
            name = f'[{name}]'

        return self.name == name

    def dict(self):
        output = {
            '@name': self.name,
            '@datatype': self.datatype,
            '@role': self.role,
            '@type': self.type

        }
        if self.semantic_role is not None:
            output['@semantic-role'] = self.semantic_role
        if self.auto_column is not None:
            output['@ns0:auto-column'] = self.auto_column
        if (self.xmlns or self.xmlns_ns0) is not None:
            output['@xmlns:ns0'] = self.xmlns or self.xmlns_ns0
        if self.ns0_auto_column is not None:
            output['@ns0:auto-column'] = self.ns0_auto_column
        if self.default_format is not None:
            output['@default-format'] = self.default_format
        if self.caption is not None:
            output['@caption'] = self.caption
        if self.aggregation is not None:
            output['@aggregation'] = self.aggregation
        if self.user_auto_column is not None:
            output['@user:auto-column'] = self.user_auto_column
        if self.default_role is not None:
            output['@default-role'] = self.default_role
        if self.default_type is not None:
            output['@default-type'] = self.default_type
        if self.param_domain_type is not None:
            output['@param-domain-type'] = self.param_domain_type
        if self.alias is not None:
            output['@alias'] = self.alias
        if self.value is not None:
            output['@value'] = self.value
        if self.ordinal is not None:
            output['@ordinal'] = str(self.ordinal)
        if self.members is not None:
            output['members'] = self.members
        if self.range is not None:
            output['range'] = self.range
        if self.hidden is not None:
            output['@hidden'] = str(self.hidden).lower()
        if self.datatype_customized is not None:
            output['@datatype-customized'] = str(self.datatype_customized).lower()
        if self.calculation is not None:
            output['calculation'] = {'@class': 'tableau', '@formula': self.calculation}
        if self.desc is not None:
            output['desc'] = {'formatted-text': {'run': self.desc}}
        if self.aliases is not None:
            output['aliases'] = dict()
            output['aliases']['alias'] = self.aliases
        if self.fiscal_year_start is not None:
            output['@fiscal-year-start'] = self.fiscal_year_start
        if self.visual_totals is not None:
            output['@visual-totals'] = self.visual_totals

        return output


@dataclass
class Relation(TableauFileObject):
    """ The Relation Tableau file object """
    type: str
    tag: str = 'relation'
    name: str = None
    table: str = None
    text: str = None
    connection: str = None
    relation: TableauFileObjects = None

    def __hash__(self):
        return hash(str(astuple(self)))

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        if isinstance(other, dict):
            return self.name == other.get('name')
        if isinstance(other, (FolderItem, object)):
            return self.name == other.name
        return False

    def __post_init__(self):
        if self.relation is not None:
            self.relation = TableauFileObjects(self.relation, item_class=Relation, tag=self.tag)
        else:
            self.relation = TableauFileObjects(item_class=Relation, tag=self.tag)
        super().__post_init__()

    def dict(self):
        dictionary = {'@type': self.type}
        if self.name is not None:
            dictionary['@name'] = self.name
        if self.connection is not None:
            dictionary['@connection'] = self.connection
        if self.table is not None:
            dictionary['@table'] = self.table
        if self.text is not None:
            dictionary['@text'] = self.text
        if self.relation is not None:
            dictionary['relation'] = [r.dict() for r in self.relation]
        return dictionary


@dataclass
class MappingCol(TableauFileObject):
    """ The mapping Col Tableau file object """
    key: str
    value: str
    tag: str = 'map'

    def __post_init__(self):
        if not re.match(r'^\[.+]$', self.key):
            self.key = f'[{self.key}]'
        if not re.match(r'^\[.+]$', self.value):
            table, column = self.value.split('.')
            self.value = f'[{table}].[{column}]'
        super().__post_init__()

    def __hash__(self):
        return hash(str(astuple(self)))

    def __eq__(self, other):
        key = ''
        value = None
        if isinstance(other, str):
            key = other
        elif isinstance(other, dict):
            key = other.get('key')
            value = other.get('value')
        elif isinstance(other, (MappingCol, object)):
            key = other.key
            value = other.value

        if not re.match(r'^\[.+]$', key):
            key = f'[{key}]'

        if value:
            return self.key == key and self.value == value
        else:
            return self.key == key

    def dict(self):
        return {'@key': self.key, '@value': self.value}


@dataclass
class FolderItem(TableauFileObject):
    """ The FolderItem Tableau file object, is an Item of the Folder.folder_item list """
    name: str
    type: str = 'field'
    tag: str = 'folder-item'

    def __hash__(self):
        return hash(str(astuple(self)))

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        if isinstance(other, dict):
            return self.name == other.get('name') and self.type == other.get('type')
        if isinstance(other, (FolderItem, object)):
            return self.name == other.name and self.type == other.type
        return False

    def dict(self):
        return {'@name': self.name, '@type': self.type}


@dataclass
class Folder(TableauFileObject):
    """ The Folder Tableau file object """
    name: str
    tag: str = 'folder'
    role: str = None
    folder_item: TableauFileObjects[FolderItem] = None

    def __post_init__(self):
        if self.folder_item is not None:
            self.folder_item = TableauFileObjects(self.folder_item, item_class=FolderItem, tag='folder-item')
        else:
            self.folder_item = TableauFileObjects(item_class=FolderItem, tag='folder-item')
        super().__post_init__()

    def __hash__(self):
        return hash(str(astuple(self)))

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        if self.role is not None:
            if isinstance(other, dict):
                return self.name == other.get('name') and self.role == other.get('role')
            if isinstance(other, (Column, object)):
                return self.name == other.name and self.role == other.role
        else:
            if isinstance(other, dict):
                return self.name == other.get('name')
            if isinstance(other, (Column, object)):
                return self.name == other.name
        return False

    def dict(self):
        output = {'@name': self.name}
        if self.role:
            output['@role'] = self.role
        if self.folder_item:
            output['folder-item'] = list()
            for folder_item in self.folder_item:
                output['folder-item'].append(folder_item.dict())
        return output


@dataclass
class FoldersCommon(TableauFileObject):
    """ The FoldersCommon Tableau file object """
    folder: TableauFileObjects[Folder] = None
    tag: str = 'folders-common'

    def __post_init__(self):
        if self.folder is not None:
            self.folder = TableauFileObjects(self.folder, item_class=Folder, tag='folder')
        else:
            self.folder = TableauFileObjects(item_class=Folder, tag='folder')
        super().__post_init__()

    def __getitem__(self, item):
        return self.folder[item]

    def __setitem__(self, key, value):
        self.folder[key] = value

    def __str__(self):
        return str(self.folder)

    def __repr__(self):
        return repr(self.folder)

    def __iter__(self):
        return iter(self.folder)

    def __eq__(self, other):
        if isinstance(other, FoldersCommon):
            other = other.folder
        for item in self.folder:
            if item not in other:
                return False
        for item in other:
            if item not in self.folder:
                return False
        return True

    def get(self, folder):
        """ Get the Folder object from FoldersCommon

        Args:
            folder (Folder|str|int): The Folder, Folder.name, or Folder index, to get

        Returns: The Folder object
        """
        return self.folder.get(folder)

    def add(self, folder):
        """ Add the Folder object to FoldersCommon

        Args:
            folder (Folder): The Folder object to add
        """
        self.folder.add(folder)

    def delete(self, folder):
        """ Delete the Folder object from FoldersCommon

        Args:
            folder (Folder|str|int): The Folder, Folder.name, or Folder index, to delete
        """
        self.folder.delete(folder)

    def update(self, folder):
        """ Update the FoldersCommon Folder object

        Args:
            folder (Folder): The Folder object to update
        """
        self.folder.update(folder)

    def dict(self):
        return {'folder': [f.dict() for f in self.folder]}


@dataclass
class DrillPath(TableauFileObject):
    """ The DrillPath Tableau file object """
    name: str
    field: list[str] = None
    tag: str = 'drill-path'

    def __hash__(self):
        return hash(str(astuple(self)))

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        else:
            if isinstance(other, dict):
                return self.name == other.get('name')
        return False

    def dict(self):
        output = {'@name': self.name}
        if self.field:
            output['field'] = self.field
        return output


@dataclass
class DrillPaths(TableauFileObject):
    """ The DrillPaths Tableau file object """
    drill_path: TableauFileObjects[DrillPath] = None
    tag: str = 'drill-paths'

    def __post_init__(self):
        if self.drill_path is not None:
            self.drill_path = TableauFileObjects(self.drill_path, item_class=DrillPath, tag='drill-path')
        else:
            self.drill_path = TableauFileObjects(item_class=DrillPath, tag='drill-path')
        super().__post_init__()

    def __getitem__(self, item):
        return self.drill_path[item]

    def __setitem__(self, key, value):
        self.drill_path[key] = value

    def __str__(self):
        return str(self.drill_path)

    def __repr__(self):
        return repr(self.drill_path)

    def __iter__(self):
        return iter(self.drill_path)

    def __eq__(self, other):
        if isinstance(other, DrillPaths):
            other = other.drill_path
        for item in self.drill_path:
            if item not in other:
                return False
        for item in other:
            if item not in self.drill_path:
                return False
        return True

    def get(self, drill_path):
        """ Get the Drill Path object from DrillPaths

        Args:
            drill_path (str): The drill path to get

        Returns: The Drill Path object
        """
        return self.drill_path.get(drill_path)

    def add(self, drill_path):
        """ Add the Drill Path object to DrillPaths

        Args:
            drill_path (str): The drill path to add
        """
        self.drill_path.add(drill_path)

    def delete(self, drill_path):
        """ Delete the Drill Path object from DrillPaths

        Args:
            drill_path (str): The drill path to delete
        """
        self.drill_path.delete(drill_path)

    def update(self, drill_path):
        """ Update the DrillPaths object

        Args:
            drill_path (str): The drill path to update
        """
        self.drill_path.update(drill_path)

    def dict(self):
        return {'drill-path': [f.dict() for f in self.drill_path]}


@dataclass
class Connection(TableauFileObject):
    """ The Connection Tableau file object """
    tag: str = 'connection'
    authentication: str = None
    class_name: str = None
    dbname: str = None
    schema: str = None
    server: str = None
    service: str = None
    username: str = None
    warehouse: str = None
    instanceurl: str = None
    odbc_connect_string_extras: str = None
    one_time_sql: str = None
    role: str = None
    query_tagging_enabled: bool = None
    saml_idp: str = None
    server_oauth: str = None
    server_userid: str = None
    workgroup_auth_mode: str = None
    tablename: str = None
    default_settings: str = None
    directory: str = None
    filename: str = None
    extract_engine: bool = None
    port: int = None

    def dict(self):
        output = dict()
        if self.authentication is not None:
            output['@authentication'] = self.authentication
        if self.class_name is not None:
            output['@class'] = self.class_name
        if self.dbname is not None:
            output['@dbname'] = self.dbname
        if self.schema is not None:
            output['@schema'] = self.schema
        if self.server is not None:
            output['@server'] = self.server
        if self.service is not None:
            output['@service'] = self.service
        if self.username is not None:
            output['@username'] = self.username
        if self.warehouse is not None:
            output['@warehouse'] = self.warehouse
        if self.instanceurl is not None:
            output['@instanceurl'] = self.instanceurl
        if self.odbc_connect_string_extras is not None:
            output['@odbc-connect-string-extras'] = self.odbc_connect_string_extras
        if self.one_time_sql is not None:
            output['@one-time-sql'] = self.one_time_sql
        if self.role is not None:
            output['@role'] = self.role
        if self.query_tagging_enabled is not None:
            output['@query-tagging-enabled'] = self.query_tagging_enabled
        if self.saml_idp is not None:
            output['@saml-idp'] = self.saml_idp
        if self.server_oauth is not None:
            output['@server-oauth'] = self.server_oauth
        if self.server_userid is not None:
            output['@server-userid'] = self.server_userid
        if self.workgroup_auth_mode is not None:
            output['@workgroup-auth-mode'] = self.workgroup_auth_mode
        if self.tablename is not None:
            output['@tablename'] = self.tablename
        if self.default_settings:
            output['@default-settings'] = self.default_settings
        if self.directory is not None:
            output['@directory'] = self.directory
        if self.filename is not None:
            output['@filename'] = self.filename
        if self.extract_engine is not None:
            output['@extract-engine'] = str(self.extract_engine).lower()
        if self.port is not None:
            output['@port'] = str(self.port)
        return output


@dataclass
class NamedConnection(TableauFileObject):
    """ The NamedConnection Tableau file object """
    name: str
    tag: str = 'named-connection'
    caption: str = None
    connection: Connection = None

    def __post_init__(self):
        if self.connection:
            self.connection = Connection(**transform_tableau_object(self.connection))
        super().__post_init__()

    def __hash__(self):
        return hash(str(astuple(self)))

    def __eq__(self, other):
        if isinstance(other, str):
            return self.connection.class_name == other
        if isinstance(other, dict):
            return self.connection.class_name == other.get('connection', {}).get('class_name')
        if isinstance(other, (NamedConnection, object)) and other.connection is not None:
            if self.connection is None:
                return other.connection is None
            return other.connection is not None and self.connection.class_name == other.connection.class_name
        return False

    def dict(self):
        return {
            '@caption': self.caption,
            '@name': self.name,
            'connection': self.connection.dict()
        }


@dataclass
class MetadataRecord(TableauFileObject):
    """ The MetadataRecord Tableau file object """
    class_name: str
    remote_name: str
    remote_type: str
    parent_name: str
    remote_alias: str
    tag: str = 'metadata-record'
    ordinal: int = None
    family: str = None
    local_type: str = None
    local_name: str = None
    aggregation: str = None
    approx_count: int = None
    width: int = None
    precision: int = None
    scale: int = None
    contains_null: bool = None
    collation: dict = None
    attributes: list = None
    object_id: str = None

    def __post_init__(self):
        if self.attributes and isinstance(self.attributes, dict):
            self.attributes = self.attributes['attribute']
        super().__post_init__()

    def __hash__(self):
        return hash(str(astuple(self)))

    def __eq__(self, other):
        if isinstance(other, str):
            return self.remote_name == other
        if isinstance(other, dict):
            return self.remote_name == other.get('remote_name')
        if isinstance(other, (Column, object)):
            return self.remote_name == other.remote_name
        return False

    def dict(self):
        output = {
            '@class': self.class_name,
            'remote-name': self.remote_name,
            'remote-type': self.remote_type,
            'local-name': self.local_name,
            'parent-name': self.parent_name,
            'remote-alias': self.remote_alias,
            'local-type': self.local_type
        }

        if self.object_id is not None:
            output['object-id'] = self.object_id
        if self.ordinal is not None:
            output['ordinal'] = self.ordinal
        if self.aggregation is not None:
            output['aggregation'] = self.aggregation
        if self.family is not None:
            output['family'] = self.family
        if self.contains_null is not None:
            output['contains-null'] = str(self.contains_null).lower()
        if self.approx_count is not None:
            output['approx-count'] = self.approx_count
        if self.scale is not None:
            output['scale'] = self.scale
        if self.precision is not None:
            output['precision'] = self.precision
        if self.width is not None:
            output['width'] = self.width
        if self.collation is not None:
            output['collation'] = self.collation
        if self.attributes is not None:
            output['attributes'] = dict()
            output['attributes']['attribute'] = self.attributes
        return output


@dataclass
class RefreshEvent(TableauFileObject):
    """ The RefreshEvent Tableau file object """
    add_from_file_path: str = None
    increment_value: str = None
    refresh_type: str = None
    rows_inserted: str = None
    timestamp_start: datetime = None

    def dict(self):
        dictionary = dict()
        if self.add_from_file_path is not None:
            dictionary['@add-from-file-path'] = self.add_from_file_path
        if self.increment_value is not None:
            dictionary['@increment-value'] = self.increment_value
        if self.refresh_type is not None:
            dictionary['@refresh-type'] = self.refresh_type
        if self.rows_inserted is not None:
            dictionary['@rows-inserted'] = self.rows_inserted
        if isinstance(self.timestamp_start, datetime):
            dictionary['@timestamp-start'] = self.timestamp_start.strftime('%Y-%m-%d %H:%M:%S.%f')
        return dictionary


@dataclass
class Refresh(TableauFileObject):
    """ The Refresh Tableau file object """
    tag: str = 'refresh'
    refresh_event: RefreshEvent = None
    increment_key: str = None
    incremental_updates: bool = None

    def __post_init__(self):
        if isinstance(self.refresh_event, dict):
            self.refresh_event = RefreshEvent(**transform_tableau_object(self.refresh_event))
        super().__post_init__()

    def dict(self):
        dictionary = dict()
        if isinstance(self.increment_key, str):
            dictionary['@increment-key'] = self.increment_key
        if isinstance(self.incremental_updates, bool):
            dictionary['@incremental-updates'] = str(self.incremental_updates).lower()
        if isinstance(self.refresh_event, RefreshEvent):
            dictionary['refresh-event'] = self.refresh_event.dict()
        else:
            dictionary['@refresh-event'] = self.refresh_event
        return dictionary


@dataclass
class ParentConnection(TableauFileObject):
    """ The parent Connection Tableau file object """
    tag: str = 'connection'
    class_name: str = None
    authentication: str = None
    access_mode: str = None
    author_locale: str = None
    dbname: str = None
    schema: str = None
    sslmode: str = None
    tablename: str = None
    username: str = None
    update_time: str = None
    extract_engine: bool = None
    named_connections: TableauFileObjects[NamedConnection] = None
    relation: Relation = None
    cols: TableauFileObjects[MappingCol] = None
    refresh: Refresh = None
    metadata_records: TableauFileObjects[MetadataRecord] = None
    default_settings: str = None

    def __post_init__(self):
        if self.refresh is not None:
            self.refresh = Refresh(**transform_tableau_object(self.refresh))
        if self.relation is not None:
            self.relation = Relation(**transform_tableau_object(self.relation))
        if self.named_connections is not None:
            self.named_connections = TableauFileObjects(
                self.named_connections['named-connection'], item_class=NamedConnection, tag='named-connections')
        else:
            self.named_connections = TableauFileObjects(item_class=NamedConnection, tag='named-connections')
        if self.cols is not None:
            self.cols = TableauFileObjects(self.cols['map'], item_class=MappingCol, tag='cols')
        else:
            self.cols = TableauFileObjects(item_class=MappingCol, tag='cols')
        if self.metadata_records is not None:
            self.metadata_records = TableauFileObjects(
                self.metadata_records['metadata-record'], item_class=MetadataRecord, tag='metadata-records'
            )
        else:
            self.metadata_records = TableauFileObjects(item_class=MetadataRecord, tag='metadata-records')
        super().__post_init__()

    def __getitem__(self, item):
        if item in self.named_connections:
            return self.named_connections[item].connection

    def __setitem__(self, key, value):
        self.named_connections[key].connection = value

    def get(self, item):
        """ Gets the TableauFileObject Connection object from the named_connection

        Args:
            item (NamedConnection|Connection|str|int): A TableauFileObject Connection object; class_name of the object;
             or index of the object

        Returns: The TableauFileObject Connection object
        """
        if isinstance(item, Connection) and item.class_name in self.named_connections:
            return self.named_connections[item.class_name].connection
        if isinstance(item, (NamedConnection, str, int)) and item in self.named_connections:
            return self.named_connections[item].connection

    def update(self, item):
        """ Updates the TableauFileObject Connection object of the named_connection

        Args:
            item (Connection): A TableauFileObject Connection object

        Returns: The TableauFileObject Connection object
        """
        if item.class_name in self.named_connections:
            self.named_connections[item.class_name].caption = item.server
            self.named_connections[item.class_name].connection = item

    def dict(self):
        dictionary = {'@class': self.class_name}
        if self.named_connections:
            dictionary['named-connections'] = {'named-connection': [nc.dict() for nc in self.named_connections]}
        if self.relation is not None:
            dictionary['relation'] = self.relation.dict()
        if self.cols is not None:
            dictionary['cols'] = {'map': [c.dict() for c in self.cols]}
        if self.refresh is not None:
            dictionary['refresh'] = self.refresh.dict()
        if self.metadata_records is not None:
            dictionary['metadata-records'] = {'metadata-record': [m.dict() for m in self.metadata_records]}
        if self.authentication is not None:
            dictionary['@authentication'] = self.authentication
        if self.access_mode is not None:
            dictionary['@access-mode'] = self.access_mode
        if self.author_locale is not None:
            dictionary['@author-locale'] = self.author_locale
        if self.dbname is not None:
            dictionary['@dbname'] = self.dbname
        if self.schema is not None:
            dictionary['@schema'] = self.schema
        if self.sslmode is not None:
            dictionary['@sslmode'] = self.sslmode
        if self.tablename is not None:
            dictionary['@tablename'] = self.tablename
        if self.username is not None:
            dictionary['@username'] = self.username
        if self.update_time is not None:
            dictionary['@update-time'] = self.update_time
        if self.extract_engine is not None:
            dictionary['@extract-engine'] = str(self.extract_engine).lower()
        return dictionary


@dataclass
class Extract(TableauFileObject):
    """ The Extract Tableau file object """
    object_id: str = None
    user_specific: bool = None
    count: int = None
    enabled: bool = None
    units: str = None
    connection: ParentConnection = None
    tag: str = 'extract'

    def __post_init__(self):
        if self.connection is not None:
            self.connection = ParentConnection(**transform_tableau_object(self.connection))
        super().__post_init__()

    def dict(self):
        dictionary = dict()
        if self.object_id is not None:
            dictionary['@object-id'] = self.object_id
        if self.user_specific is not None:
            dictionary['@user-specific'] = str(self.user_specific).lower()
        if self.count is not None:
            dictionary['@count'] = self.count
        if self.enabled is not None:
            dictionary['@enabled'] = str(self.enabled).lower()
        if self.units is not None:
            dictionary['@units'] = self.units
        if self.connection is not None:
            dictionary['connection'] = self.connection.dict()
        return dictionary


@dataclass
class Aliases(TableauFileObject):
    """ The Aliases Tableau file object """
    enabled: bool = True
    tag: str = 'aliases'

    def dict(self):
        return {'@enabled': 'yes' if self.enabled else 'no'}


@dataclass
class DateOptions(TableauFileObject):
    """ The DateOptions Tableau file object """
    fiscal_year_start: str = None
    start_of_week: str = None
    tag: str = 'date-options'

    def dict(self):
        dictionary = dict()
        if self.fiscal_year_start is not None:
            dictionary['@fiscal-year-start'] = self.fiscal_year_start
        if self.fiscal_year_start is not None:
            dictionary['@start-of-week'] = self.start_of_week
        return dictionary


@dataclass
class ColumnInstance(TableauFileObject):
    """ The ColumnInstance Tableau file object """
    column: str = None
    derivation: str = None
    name: str = None
    pivot: str = None
    type: str = None
    tag: str = 'column-instance'

    def dict(self):
        dictionary = dict()
        if self.column is not None:
            dictionary['@column'] = self.column
        if self.derivation is not None:
            dictionary['@derivation'] = self.derivation
        if self.name is not None:
            dictionary['@name'] = self.name
        if self.pivot is not None:
            dictionary['@pivot'] = self.pivot
        if self.type is not None:
            dictionary['@type'] = self.type
        return dictionary


if __name__ == '__main__':
    t1 = Column(name='Test', datatype='integer', role='measure', type='quantitative', calculation='COUNT(1)')
    t2 = Column(name='Test', datatype='string', role='dimension', type='ordinal')
    print(t1 == 'Test')
    print(t1 == {'name': 'Test'})
    print(t1 == t2)
    print(t1.dict())
    print(t2.dict())
