import requests
from tableau_utilities.tableau_server.base import Base
from tableau_utilities.tableau_server.get import Get
from tableau_utilities.tableau_server.create import Create
from tableau_utilities.tableau_server.download import Download
from tableau_utilities.tableau_server.publish import Publish
from tableau_utilities.tableau_server.refresh import Refresh
from tableau_utilities.tableau_server.update import Update
from tableau_utilities.tableau_server.static import TableauConnectionError


class TableauServer(Base):
    """ Connects and interacts with Tableau Online/Server, via the REST API. """

    def __init__(
            self,
            host: str,
            site: str,
            user: str = None,
            password: str = None,
            personal_access_token_secret: str = None,
            personal_access_token_name: str = None,
            api_version: float = None
    ):
        """ To sign in to Tableau a user needs either a username & password or token secret & token name

        Args:
            host: Tableau server address
            user: The username to sign in to Tableau Online with
            password: The password to sign in to Tableau Online with
            personal_access_token_name: The name of the personal access token used
            personal_access_token_secret: The secret of the personal access token used
            site: The Tableau Online site id
            api_version: The Tableau REST API version
        """
        self.user = user
        self._pw = password
        self._personal_access_token_secret = personal_access_token_secret
        self.personal_access_token_name = personal_access_token_name
        self.host = host
        self.site = site
        self.api = api_version or 3.18
        # Set by class
        self._auth_token = None
        self.url: str = None
        # Create a session on initialization
        self.session = requests.session()
        self.session.headers.update({'accept': 'application/json', 'content-type': 'application/json'})
        super().__init__(self)
        # Sign in on initialization
        self.__sign_in()
        # Assign core functionality
        self.get: Get = Get(self)
        self.create: Create = Create(self)
        self.download: Download = Download(self)
        self.publish: Publish = Publish(self)
        self.refresh: Refresh = Refresh(self)
        self.update: Update = Update(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Sign out upon class deconstruction
        print('Signing out of Tableau Server')
        self.sign_out()

    def __sign_in(self):
        """
            To sign in to Tableau a user needs either a username & password or token secret & token name
            Signs in to the server with credentials from the specified connection.
            Sets the auth_token, site_id, and url common prefix

        """
        url = f"{self.host}/api/{self.api}/auth/signin"
        if self._personal_access_token_secret and self.personal_access_token_name:
            body = {"credentials": {"personalAccessTokenSecret": self._personal_access_token_secret,
                                    "personalAccessTokenName": self.personal_access_token_name,
                                    "site": {"contentUrl": self.site}}}
        elif self.user and self._pw:
            body = {"credentials": {"name": self.user, "password": self._pw, "site": {"contentUrl": self.site}}}
        else:
            raise TableauConnectionError(
                'Please provide either user and password, or token_secret and token_name'
            )

        res = self._post(url, json=body).get('credentials', {})
        # Set auth token and site ID attributes on sign in
        self.session.headers.update({'x-tableau-auth': res.get('token')})
        self.site = res.get('site', {}).get('id')
        self.url = f"{self.host}/api/{self.api}/sites/{self.site}"

    def sign_out(self):
        """ Destroys the active session and invalidates authentication token. """
        self._post(url=f"{self.host}/api/{self.api}/auth/signout")
        self.session.close()

    def embed_datasource_credentials(self, datasource_id, credentials, connection_type):
        """ Embed the given credentials for all connections of a datasource of the given connection type.
            Only embeds Username and Password credentials.

        Args:
            datasource_id (str): The ID of the datasource
            connection_type (str): Type of connection you want to embed creds for, i.e. snowflake
            credentials (dict): The credentials dict to embed
                i.e. {'username': 'user', 'password': 'password'}
        """
        for cred in ['username', 'password']:
            if not credentials.get(cred):
                raise TableauConnectionError(f'Missing required credential: {cred}')

        for c in self.get.datasource_connections(datasource_id):
            if c.type.lower() == connection_type.lower():
                c.user_name = credentials['username']
                c.password = credentials['password']
                c.embed_password = True
                response = self.update.datasource_connection(datasource_id, c)
                return response
