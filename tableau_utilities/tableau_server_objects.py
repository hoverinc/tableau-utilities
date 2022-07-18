import xmltodict
from datetime import datetime
from dataclasses import dataclass, asdict, astuple, field


@dataclass
class TableauObject:
    """ The minimum attributes and functionality of a Tableau object """
    id: str = ''
    name: str = ''

    def __hash__(self):
        return hash(str(astuple(self)))

    def to_dict(self):
        return asdict(self)


@dataclass
class User(TableauObject):
    """ A Tableau User object """
    domain: dict = field(default=dict)
    authSetting: str = ''
    email: str = ''
    emailDomain: str = ''
    externalAuthUserId: str = ''
    fullName: str = ''
    lastLogin: datetime = None
    siteRole: str = ''
    locale: str = ''
    language: str = 'en'

    def __post_init__(self):
        if isinstance(self.lastLogin, str):
            self.lastLogin = datetime.strptime(self.lastLogin, '%Y-%m-%dT%H:%M:%SZ')
        if not self.emailDomain and self.email:
            self.emailDomain = self.email.lower().split('@')[-1]

    def __hash__(self):
        return hash(str(astuple(self)))


@dataclass
class Project(TableauObject):
    """ A Tableau Project object """
    domain: dict = field(default=dict)
    owner: [User] = User()
    parentProjectId: str = ''
    description: str = ''
    controllingPermissionsProjectId: str = ''
    createdAt: datetime = None
    updatedAt: datetime = None
    contentPermissions: str = ''

    def __post_init__(self):
        if isinstance(self.createdAt, str):
            self.createdAt = datetime.strptime(self.createdAt, '%Y-%m-%dT%H:%M:%SZ')
        if isinstance(self.updatedAt, str):
            self.updatedAt = datetime.strptime(self.updatedAt, '%Y-%m-%dT%H:%M:%SZ')

    def __hash__(self):
        return hash(str(astuple(self)))


@dataclass
class Group(TableauObject):
    """ A Tableau Group object """
    domain: dict = field(default=dict)
    imported: dict = field(default=dict)

    def __hash__(self):
        return hash(str(astuple(self)))


@dataclass
class Datasource(TableauObject):
    """ A Tableau Datasource object """
    createdAt: datetime = None
    updatedAt: datetime = None
    project_id: str = ''
    project_name: str = ''
    owner: User = User()
    tags: list = field(default_factory=list)
    description: str = ''
    certificationNote: str = ''
    contentUrl: str = ''
    webpageUrl: str = ''
    type: str = ''
    serverName: str = ''
    databaseName: str = ''
    encryptExtracts: bool = False
    hasAlert: bool = False
    hasExtracts: bool = False
    isCertified: bool = False
    isPublished: bool = True
    useRemoteQueryAgent: bool = False

    def __post_init__(self):
        if isinstance(self.createdAt, str):
            self.createdAt = datetime.strptime(self.createdAt, '%Y-%m-%dT%H:%M:%SZ')
        if isinstance(self.updatedAt, str):
            self.updatedAt = datetime.strptime(self.updatedAt, '%Y-%m-%dT%H:%M:%SZ')
        if isinstance(self.encryptExtracts, str):
            self.encryptExtracts = self.encryptExtracts.lower() == 'true'

    def publish_xml(self, conn_creds=None):
        xml_dict = {
            'tsRequest': {
                'datasource': {
                    '@name': self.name,
                    '@useRemoteQueryAgent': str(self.useRemoteQueryAgent).lower(),
                    '@description': self.description,
                    'project': {'@id': self.project_id}
                }
            }
        }
        if conn_creds:
            xml_dict['tsRequest']['datasource']['connectionCredentials'] = {
                '@name': conn_creds['username'],
                '@password': conn_creds['password'],
                '@embed': "true"
            }
        return xmltodict.unparse(xml_dict)


@dataclass
class Connection(TableauObject):
    """ A Tableau Connection object """
    datasource_id: set = ''
    datasource_name: set = ''
    type: str = ''
    serverAddress: str = ''
    serverPort: str = ''
    userName: str = ''
    password: str = ''
    embedPassword: bool = False
    queryTaggingEnabled: bool = False

    def to_dict(self):
        conn_dict = super().to_dict()
        del conn_dict['type']
        del conn_dict['datasource_id']
        del conn_dict['datasource_name']
        return conn_dict


@dataclass
class View(TableauObject):
    """ A Tableau View object """
    createdAt: datetime = None
    updatedAt: datetime = None
    tags: list = field(default_factory=list)
    contentUrl: str = ''
    viewUrlName: str = ''

    def __post_init__(self):
        if isinstance(self.createdAt, str):
            self.createdAt = datetime.strptime(self.createdAt, '%Y-%m-%dT%H:%M:%SZ')
        if isinstance(self.updatedAt, str):
            self.updatedAt = datetime.strptime(self.updatedAt, '%Y-%m-%dT%H:%M:%SZ')


@dataclass
class Workbook(TableauObject):
    """ A Tableau Workbook object """
    createdAt: datetime = None
    updatedAt: datetime = None
    project_id: str = ''
    project_name: str = ''
    owner: User = User()
    location: dict = field(default_factory=dict)
    tags: list = field(default_factory=list)
    dataAccelerationConfig: dict = field(default_factory=dict)
    defaultViewId: str = ''
    description: str = ''
    contentUrl: str = ''
    webpageUrl: str = ''
    size: int = 0
    showTabs: bool = False
    encryptExtracts: bool = False
    views: list = field(default_factory=list)

    def __post_init__(self):
        if isinstance(self.createdAt, str):
            self.createdAt = datetime.strptime(self.createdAt, '%Y-%m-%dT%H:%M:%SZ')
        if isinstance(self.updatedAt, str):
            self.updatedAt = datetime.strptime(self.updatedAt, '%Y-%m-%dT%H:%M:%SZ')
        if isinstance(self.encryptExtracts, str):
            self.encryptExtracts = self.encryptExtracts.lower() == 'true'
        if isinstance(self.showTabs, str):
            self.showTabs = self.showTabs.lower() == 'true'

    def publish_xml(self, conns=None):
        xml_dict = {
            'tsRequest': {
                'workbook': {
                    '@name': self.name,
                    '@showTabs': str(self.showTabs).lower(),
                    '@thumbnailsUserId': self.owner.id,
                    'project': {'@id': self.project_id}
                }
            }
        }
        if conns:
            xml_dict['tsRequest']['workbook'].setdefault('connections', list())
            for conn in conns:
                xml_dict['tsRequest']['workbook']['connections'].append({
                    'connection': {
                        '@serverAddress': conn['address'],
                        '@serverPort': conn['port'],
                        'connectionCredentials': {
                            '@name': conn['username'],
                            '@password': conn['password'],
                            '@embed': "true"
                        }
                    }
                })
        return xmltodict.unparse(xml_dict)


@dataclass
class Job:
    createdAt: datetime
    id: str
    mode: str
    type: str
    datasource_id: str = ''
    datasource_name: str = ''
    workbook_id: str = ''
    workbook_name: str = ''

    def __post_init__(self):
        if isinstance(self.createdAt, str):
            self.createdAt = datetime.strptime(self.createdAt, '%Y-%m-%dT%H:%M:%SZ')


if __name__ == '__main__':
    u1 = User(domain={'test': 1}, name='Bob')
    u2 = User(domain={'test': 1}, name='Bob')
    u3 = User(domain={'test': 1}, name='Tom')
    for u in {u1, u2, u3}:
        print(u.to_dict())
