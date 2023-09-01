import logging
import re
import yaml
from github import Github, Auth
from dataclasses import dataclass, field, asdict


class CFGList(list):
    """ Extends the functionality of a list to get and set items by equality """
    def __getitem__(self, item):
        if isinstance(item, int):
            return super().__getitem__(item)
        return super().__getitem__(self.index(item))

    def __setitem__(self, item, value):
        if isinstance(item, int):
            super().__setitem__(item, value)
        else:
            super().__setitem__(self.index(item), value)

    def get(self, item):
        """ Gets the item from the list """
        for obj in self:
            if obj == item:
                return item
        return None


@dataclass
class CFGColumn:
    """ A Column in the columns and calculations YAML files """
    name: str
    caption: str
    role: str
    type: str
    datatype: str
    remote_name: str = None
    desc: str = None
    calculation: str = None
    folder_name: str = None
    metadata: dict = None

    def __post_init__(self):
        # Surround the column local-name with brackets
        if not re.search(r'^\[.+?]', self.name):
            self.name = f'[{self.name}]'

    def __eq__(self, other):
        if isinstance(other, str):
            return self.caption == other
        if isinstance(other, dict):
            return self.caption == other.get('caption') \
                and self.name == other.get('name') \
                and self.role == other.get('role') \
                and self.type == other.get('type') \
                and self.datatype == other.get('datatype') \
                and self.desc == other.get('desc') \
                and self.calculation == other.get('calculation')
        if isinstance(other, (CFGColumn, object)):
            return self.caption == other.caption \
                and self.name == other.name \
                and self.role == other.role \
                and self.type == other.type \
                and self.datatype == other.datatype \
                and self.desc == other.desc \
                and self.calculation == other.calculation
        return False

    def dict(self):
        """ Returns a dict of the class """
        dictionary = asdict(self)
        del dictionary['metadata']
        return dictionary


@dataclass
class CFGFolder:
    """ A Folder of Columns in the columns and calculations YAML files """
    name: str
    role: str = None

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        if isinstance(other, dict):
            return self.name == other.get('name') and self.role == other.get('role') \
                or not other.get('role') and self.name == other.get('name')
        if isinstance(other, (CFGFolder, object)):
            return self.name == other.name and self.role == other.role \
                or not other.role and self.name == other.name
        return False

    def dict(self):
        """ Returns a dict of the class """
        return asdict(self)


@dataclass
class CFGDatasource:
    """ A Datasource of a Column in the columns and calculations YAML files """
    name: str
    project_name: str
    id: str = None
    columns: CFGList[CFGColumn] = field(default_factory=CFGList)
    folders: CFGList[CFGFolder] = field(default_factory=CFGList)

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        if isinstance(other, dict):
            return self.name == other.get('name') and self.project_name == other.get('project_name')
        if isinstance(other, (CFGDatasource, object)):
            return self.name == other.name and self.project_name == other.project_name
        return False


class Config:
    """ Creates a configuration of the Tableau Datasource(s),
        based on the corresponding DBT model YAML file(s) in a separate GitHub repo.

    Args:
        githup_token (str): The GitHub token with scope to read from the DBT repo
        repo_name (str): The name of the DBT GitHub repo
        repo_branch (str): The branch name of the GitHub repo
        subfolder (str): The sub-folder in the GitHub repo which contains the Datasource yaml file(s)
    """
    def __init__(self, githup_token, repo_name, subfolder, repo_branch='main'):
        self.__github_token = githup_token
        self._repo_name = repo_name
        self._repo_branch = repo_branch
        self._yaml_subfolder = subfolder
        with open('dags/tableau_datasource_update/configs/column_personas.yaml') as read_config:
            self._personas = yaml.safe_load(read_config)

        # Set the datasources configuration based on the config files
        self.datasources: CFGList[CFGDatasource] = CFGList()
        self.__set_datasources()
        self.__update_column_calcs()

    def __set_datasources(self):
        """
            Reads the YAML files from DBT to form the datasources attribute.
            Output -> {datasource: {column_caption: info}}
        """
        # Read the files from GitHub
        auth = Auth.Token(self.__github_token)
        github = Github(auth=auth)
        repo = github.get_repo(self._repo_name)
        files = repo.get_contents(self._yaml_subfolder, ref=self._repo_branch)
        for file in files:
            # Parse YAML files
            name, extension = file.name.split('.')
            if extension.lower() not in ['yaml', 'yml']:
                continue
            model_cfg = yaml.safe_load(file.decoded_content.decode())
            model_meta_tableau = model_cfg['models'][0].get('meta', {}).get('tableau', None)
            if not model_meta_tableau:
                logging.warning('(Skipping) Datasource model does not contain tableau meta data: %s', name)
                continue
            calculations = model_meta_tableau.pop('calculations', [])
            columns = model_cfg['models'][0].pop('columns', [])
            datasource = CFGDatasource(
                name=model_meta_tableau.pop('datasource'),
                project_name=model_meta_tableau.pop('project')
            )
            # Add metadata columns
            for column in columns:
                column_meta_tableau = column.get('meta', {}).get('tableau')
                if not column_meta_tableau:
                    logging.warning('(Skipping) Column does not contain tableau meta data: %s / %s -> %s',
                                    datasource.project_name, datasource.name, column['name'])
                    continue
                persona = self._personas[column_meta_tableau['persona']]
                desc = column['description'].strip() if column.get('description') else None
                metadata = {
                    'class_name': 'column',
                    'remote_name': column['name'],
                    'remote_alias': column['name'],
                    'local_name': column_meta_tableau['local_name'],
                    'local_type': persona['datatype'],
                    'object_id': '[Migrated Data]',
                    'contains_null': True
                }
                metadata.update(persona['metadata'])
                cfg_column = CFGColumn(
                    name=column_meta_tableau['local_name'],
                    caption=column_meta_tableau['alias'],
                    role=persona['role'],
                    type=persona['role_type'],
                    datatype=persona['datatype'],
                    desc=desc,
                    folder_name=column_meta_tableau['folder'],
                    remote_name=column['name'],
                    metadata=metadata
                )
                if cfg_column.folder_name not in datasource.folders:
                    datasource.folders.append(CFGFolder(name=cfg_column.folder_name, role=cfg_column.role))
                datasource.columns.append(cfg_column)
            # Add tableau calculation columns
            for column in calculations:
                desc = column['description'].strip() if column.get('description') else None
                cfg_column = CFGColumn(
                    name=column['local_name'],
                    caption=column['alias'],
                    role=self._personas[column['persona']]['role'],
                    type=self._personas[column['persona']]['role_type'],
                    datatype=self._personas[column['persona']]['datatype'],
                    calculation=column['calculation'],
                    desc=desc,
                    folder_name=column['folder']
                )
                if cfg_column.folder_name not in datasource.folders:
                    datasource.folders.append(CFGFolder(name=cfg_column.folder_name, role=cfg_column.role))
                datasource.columns.append(cfg_column)
            self.datasources.append(datasource)

    def __replace_caption_references(self, datasource_name: str, column: CFGColumn):
        """
          Replaces captions in a column calculation with their local-name,
          when calculation is provided
        """
        # Get all distinct captions referenced in the calc
        calc_captions = set(re.findall(r'\[.+?]', column.calculation))
        # Replace each caption, with the corresponding local-name defined in the column_config
        for calc_caption in calc_captions:
            caption = re.sub(r'[\[\]]+', '', calc_caption)
            local_name = self.datasources[datasource_name].columns[caption].name
            column.calculation = column.calculation.replace(calc_caption, local_name)
        return column.calculation

    def __update_column_calcs(self):
        """
          Updates the calculation for each column to reference the local-name of a column,
          rather than caption
        """
        for datasource in self.datasources:
            for column in datasource.columns:
                if not column.calculation:
                    continue
                new_calc = self.__replace_caption_references(datasource.name, column)
                self.datasources[datasource.name].columns[column.caption].calculation = new_calc
