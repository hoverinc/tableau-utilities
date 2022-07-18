import logging
import requests
import cgi
import os
import json

from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata
from tableau_server_objects import User, Project, Group, Datasource, Connection, View, Workbook, Job
# Imports for development
from dotenv import load_dotenv


class TableauConnectionError(Exception):
    """ An Exception in the TableauServer connection """
    pass


class TableauServer:
    """
        Creates a Tableau connection,
        with functionality to interact with the Tableau Online, via the REST API.
    """

    def __init__(self, user, password, host, site, api_version=3.16):
        """
        Args:
            host (str): Tableau server address
            user (str): The username to sign in to Tableau Online with
            password (str): The password to sign in to Tableau Online with
            site (str): The Tableau Online site id to sign in to
            api_version (float): The api version to use
        """
        self.user = user
        self.__pw = password
        self.host = host
        self.site = site
        self.api = api_version
        # Set by class
        self._auth_token = None
        self.url = None
        # Sign in on initialization
        self.__sign_in()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Sign out upon class deconstruction
        self.__sign_out()

    def __sign_in(self):
        """
            Signs in to the server with credentials from the specified connection.
            Sets the auth_token, site_id, and url common prefix
        """

        url = f"{self.host}/api/{self.api}/auth/signin"
        body = {"credentials": {"name": self.user, "password": self.__pw, "site": {"contentUrl": self.site}}}

        print(f"\nSigning in to Tableau Online: {self.host} {self.user} {self.site} {self.api}\n", )
        res = self.post(url, json=body).get('credentials', {})
        # Set auth token and site ID attributes on sign in
        self._auth_token = res.get('token')
        self.site = res.get('site', {}).get('id')
        self.url = f"{self.host}/api/{self.api}/sites/{self.site}"

    def __sign_out(self):
        """ Destroys the active session and invalidates authentication token. """
        print("\nSigning out and invalidating the authentication token")
        self.post(url=f"{self.host}/api/{self.api}/auth/signout")

    def __apply_default_header(self, headers=None):
        """ Applies some default headers used in all API calls """
        default_headers = {
            'accept': 'application/json',
            'x-tableau-auth': self._auth_token
        }
        if headers:
            default_headers.update(headers)
        return default_headers

    @staticmethod
    def __validate_response(response):
        """ Validates the response received from an API call

        Args:
            response (object): A response object

        Returns: The response content as a JSON dict
        """
        info = json.loads(response.content) if response.content else {}
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            error = info.get('error', {})
            raise TableauConnectionError(
                f'\nError: {error.get("code")}: {error.get("summary")} - {error.get("detail")}\n{err}'
            ) from err
        return info

    def get(self, url, headers=None, **params):
        """ GET request for the Tableau REST API

        Args:
            url (str): URL endpoint for GET call
            headers (dict): GET call header

        Returns: The response content as a JSON dict
        """

        res = requests.get(url, headers=self.__apply_default_header(headers), **params)
        return self.__validate_response(res)

    def delete(self, url, headers=None, **params):
        """ DELETE request for the Tableau REST API

        Args:
            url (str): URL endpoint for DELETE call
            headers (dict): DELETE call header

        Returns: The response content as a JSON dict
        """
        res = requests.delete(url, headers=self.__apply_default_header(headers), **params)
        return self.__validate_response(res)

    def post(self, url, json=None, headers=None, **params):
        """ POST request for the Tableau REST API

        Args:
            url (str): URL endpoint for POST call
            json (dict): The POST call JSON payload
            headers (dict): POST call header

        Returns: The response content as a JSON dict
        """
        res = requests.post(url, json=json, headers=self.__apply_default_header(headers), **params)
        return self.__validate_response(res)

    def put(self, url, json=None, headers=None, **params):
        """ PUT request for the Tableau REST API

        Args:
            url (str): URL endpoint for PUT call
            json (dict): The PUT call JSON payload
            headers (dict): PUT call header

        Returns: The response content as a JSON dict
        """
        res = requests.put(url, json=json, headers=self.__apply_default_header(headers), **params)
        return self.__validate_response(res)

    def __get_objects_pager(self, url, obj, page_size=100):
        """ GET all objects in the site
        Args:
            url: The url of for the request, i.e /api/api-version/sites/site-id/groups
            obj: The name of the object being requested
            page_size: The size of the page (number of objects per page)
        Returns: A list of the objects
        """
        response = self.get(url)
        total_available = response.get('pagination', {}).get('totalAvailable')
        total_available = int(total_available)
        current = 0
        page = 0
        while current < total_available:
            current += page_size
            page += 1
            page_url = f'{url}?pageSize={page_size}&pageNumber={page}'
            logging.info('GET %s --> %s/%s', page_url, current, total_available)
            res = self.get(page_url)
            for obj_dict in res[f'{obj}s'][obj]:
                yield obj_dict

    def __transform_datasource(self, datasource_dict):
        """ Transforms the datasource dict from a Tableau REST API call

        Args:
            datasource_dict (dict): The datasource dict from a Tableau REST API call
        """
        if datasource_dict.get('project'):
            datasource_dict['project_id'] = datasource_dict.get('project', {}).get('id')
            datasource_dict['project_name'] = datasource_dict.get('project', {}).get('name')
            del datasource_dict['project']
        owner_id = datasource_dict.get('owner', {}).get('id')
        if owner_id:
            datasource_dict['owner'] = self.get_user(owner_id)
        if datasource_dict.get('tags'):
            datasource_dict['tags'] = [t['label'] for t in datasource_dict['tags']['tag']]

    def __transform_workbook(self, workbook_dict):
        """ Transforms the workbook dict from a Tableau REST API call

        Args:
            workbook_dict (dict): The workbook dict from a Tableau REST API call
        """
        if workbook_dict.get('project'):
            workbook_dict['project_id'] = workbook_dict['project']['id']
            workbook_dict['project_name'] = workbook_dict['project']['name']
            del workbook_dict['project']
        owner_id = workbook_dict.get('owner', {}).get('id')
        if owner_id:
            workbook_dict['owner'] = self.get_user(owner_id)
        if workbook_dict.get('tags'):
            workbook_dict['tags'] = [t['label'] for t in workbook_dict['tags']['tag']]
        if workbook_dict.get('views'):
            workbook_dict['views'] = [View(**v) for v in workbook_dict['views']['view']]

    @staticmethod
    def __transform_job(job_dict):
        """ Transforms the job dict from a Tableau REST API call

        Args:
            job_dict (dict): The job dict from a Tableau REST API call
        """
        datasource = job_dict.get('extractRefreshJob', {}).get('datasource')
        workbook = job_dict.get('extractRefreshJob', {}).get('workbook')
        if workbook:
            job_dict['workbook_id'] = workbook['id']
            job_dict['workbook_name'] = workbook['name']
            del job_dict['extractRefreshJob']
        if datasource:
            job_dict['datasource_id'] = datasource['id']
            job_dict['datasource_name'] = datasource['name']
            del job_dict['extractRefreshJob']

    def get_datasource_connections(self, datasource_id):
        """ Queries for all Connection Tableau objects in the datasource
            URI GET /api/api-version/sites/site-id/datasources/datasource_id/connections
        Args:
            datasource_id (str): The ID of the Tableau Datasource
        Returns: All Connection Tableau objects in the datasource
        """
        res = self.get(f'{self.url}/datasources/{datasource_id}/connections')
        for connection in res['connections']['connection']:
            if connection.get('datasource'):
                connection['datasource_id'] = connection['datasource']['id']
                connection['datasource_name'] = connection['datasource']['name']
                del connection['datasource']
            yield Connection(**connection)

    def get_datasource(self, datasource_id):
        """ Queries for a datasource in the site
            URI GET /api/api-version/sites/site-id/datasources/datasource_id
        Returns: A Datasource Tableau object
        """
        d = self.get(f'{self.url}/datasources/{datasource_id}')
        d = d['datasource']
        self.__transform_datasource(d)
        return Datasource(**d)

    def get_datasources(self):
        """ Queries for all datasources in the site
            URI GET /api/api-version/sites/site-id/datasources
        Returns: All datasources in the site
        """
        for d in self.__get_objects_pager(f"{self.url}/datasources", 'datasource'):
            self.__transform_datasource(d)
            yield Datasource(**d)

    def get_workbook_connections(self, workbook_id):
        """ Queries for all Connection Tableau objects in the workbook
            URI GET /api/api-version/sites/site-id/workbooks/workbook_id/connections
        Args:
            workbook_id (str): The ID of the Tableau Workbook
        Returns: All Connection Tableau objects in the workbook
        """
        res = self.get(f'{self.url}/workbooks/{workbook_id}/connections')
        for connection in res['connections']['connection']:
            connection['datasource_id'] = connection['datasource']['id']
            connection['datasource_name'] = connection['datasource']['name']
            del connection['datasource']
            yield Connection(**connection)

    def get_workbook(self, workbook_id):
        """ Queries for a workbook in the site
            URI GET /api/api-version/sites/site-id/workbooks/workbook_id
        Args:
            workbook_id (str): The ID of the Tableau Workbook
        Returns: A Workbooks Tableau object
        """
        w = self.get(f'{self.url}/workbooks/{workbook_id}')
        w = w['workbook']
        self.__transform_workbook(w)
        return Workbook(**w)

    def get_workbooks(self):
        """ Queries for all workbooks in the site
            URI GET /api/api-version/sites/site-id/workbooks
        Returns: All workbooks in the site
        """
        for w in self.__get_objects_pager(f"{self.url}/workbooks", 'workbook'):
            self.__transform_workbook(w)
            yield Workbook(**w)

    def get_view(self, view_id):
        """ Queries for a view in the site
            URI GET /api/api-version/sites/site-id/views/view_id
        Args:
            view_id (str): The ID of the Tableau View
        Returns: A View Tableau object
        """
        v = self.get(f'{self.url}/views/{view_id}')
        v = v['view']
        return View(**v)

    def get_views(self):
        """ Queries for all views in the site
            URI GET /api/api-version/sites/site-id/views
        Returns: All workbooks in the site
        """
        for v in self.__get_objects_pager(f"{self.url}/views", 'view'):
            yield View(**v)

    def get_project(self, project_id):
        """ Queries for the project by project_id
            URI GET /api/api-version/sites/site-id/projects/project_id
        Args:
            project_id (str): The ID of the project in Tableau Online
        Returns: A Tableau Project object specified by ID
        """
        for p in self.get_projects():
            if p.id == project_id:
                return p

    def get_projects(self, top_level_only=True):
        """ Queries for all projects in the site
            URI GET /api/api-version/sites/site-id/projects
        Args:
            top_level_only (bool): True to only get top level projects
        Returns: All top level projects in the site
        """
        for p in self.__get_objects_pager(f"{self.url}/projects", 'project'):
            # Only get Top Level projects
            if top_level_only and p.get('parentProjectId'):
                continue
            # Get the Tableau User of the project's owner
            owner = p.pop('owner')
            p.update({'owner': self.get_user(owner['id'])})
            yield Project(**p)

    def get_group(self, group_id):
        """ Queries for the group by user_id
            URI GET /api/api-version/sites/site-id/groups/group_id
        Args:
            group_id (str): The ID of the group in Tableau Online
        Returns: A Tableau Group object specified by ID
        """
        for group in self.get_groups():
            if group.id == group_id:
                return group

    def get_groups(self):
        """ Queries for all groups in the site
            URI GET /api/api-version/sites/site-id/groups
        Returns: All groups in the site
        """
        for g in self.__get_objects_pager(f"{self.url}/groups", 'group'):
            # Rename attribute from import to imported, as import is reserved by Python
            g.update({'imported': g.pop('import', None)})
            yield Group(**g)

    def get_user(self, user_id):
        """ Queries for the user by user_id
            URI GET /api/api-version/sites/site-id/users/user_id
        Args:
            user_id (str): The ID of the user in Tableau Online
        Returns: A Tableau User object specified by ID
        """
        u = self.get(f"{self.url}/users/{user_id}")
        return User(**u['user'])

    def get_users(self):
        """ Queries for all users in the site
            URI GET /api/api-version/sites/site-id/users
        Returns: All users in the site
        """
        for user in self.__get_objects_pager(f"{self.url}/users", 'user'):
            yield User(**user)

    def get_user_groups(self):
        """ Queries for all groups and all user in those groups
            URI GET /api/api-version/sites/site-id/groups/group_id/users
        Returns: A list of all user/group combinations
        """
        for group in self.get_groups():
            for u in self.__get_objects_pager(f"{self.url}/groups/{group.id}/users", 'user'):
                yield group, User(**u)

    def create_project(self, name, detail=None):
        """ Creates a project.
        Args:
            name (str): Name of the project.
            detail (dict): Dictionary of additional detail,
                i.e. description and contentPermissions
        """
        if not detail:
            detail = dict()
        obj_payload = {
            'project': {
                'name': name,
                'description': detail.get('description', ''),
                'contentPermissions': detail.get('contentPermissions', 'LockedToProject')
            }
        }
        self.post(f'{self.url}/projects', obj_payload)

    def create_group(self, name, detail=None):
        """ Creates a group.
        Args:
            name (str): Name of the group.
            detail (dict): Dictionary of additional detail,
                i.e. minimumSiteRole
        """
        if not detail:
            detail = dict()
        obj_payload = {
            'group': {
                'name': name,
                'minimumSiteRole': detail.get('minimumSiteRole', 'Viewer')
            }
        }
        self.post(f'{self.url}/groups', obj_payload)

    def __download_object(self, url, file_dir):
        """ Downloads a datasource from Tableau Online

        Args:
            url (str): The URL for the request
            file_dir (str): The file directory to write the file to

        Returns: The absolute path to the file
        """
        res = requests.get(url, headers=self.__apply_default_header())
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise TableauConnectionError(err) from err
        _, params = cgi.parse_header(res.headers["Content-Disposition"])
        file_name = os.path.basename(params["filename"])
        os.makedirs(file_dir, exist_ok=True)
        path = os.path.join(file_dir, file_name)
        with open(path, "wb") as f:
            # Download in 1024 bytes (1kb) chunks
            for chunk in res.iter_content(1024):
                f.write(chunk)
        return os.path.abspath(path)

    def download_datasource(self, datasource_id, file_dir='', include_extract=False):
        """ Downloads a datasource from Tableau Online

        Args:
            datasource_id (str):
            file_dir (str):
            include_extract (bool):
        """
        return self.__download_object(
            f'{self.url}/datasources/{datasource_id}/content?includeExtract={include_extract}',
            file_dir
        )

    def download_workbook(self, workbook_id, file_dir='', include_extract=False):
        """ Downloads a workbook from Tableau Online

        Args:
            workbook_id (str):
            file_dir (str):
            include_extract (bool):
        """
        return self.__download_object(
            f'{self.url}/workbooks/{workbook_id}/content?includeExtract={include_extract}',
            file_dir
        )

    @staticmethod
    def __get_multipart_details(parts):
        """ Gets the body and content_type for a multipart/mixed request.

        Args:
            parts (list[tuple[str, str, str, str]]): The parts that make up the RequestField
                i.e. [(name, data, file_name, content_type)]

        Returns: Request body and content_type
        """
        part_list = list()
        for name, data, file_name, content_type in parts:
            r = RequestField(name=name, data=data, filename=file_name)
            r.make_multipart(content_type='application/octet-stream')
            part_list.append(r)
        post_body, content_type = encode_multipart_formdata(part_list)
        content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
        return post_body, content_type

    def __get_datasource_for_publication(self, datasource_id, datasource_name, project_name):
        if datasource_id:
            return self.get_datasource(datasource_id)
        elif datasource_name and project_name:
            project = None
            for p in self.get_projects():
                if p.name == project_name:
                    project = p
                    break
            if not project:
                raise TableauConnectionError(f'Project does not exist: {project}')
            return Datasource(
                name=datasource_name,
                project_id=project.id,
                project_name=project.name
            )
        else:
            raise TableauConnectionError('Specify datasource_id or datasource_name and project_name')

    def __get_workbook_for_publication(self, workbook_id, workbook_name, project_name):
        if workbook_id:
            return self.get_workbook(workbook_id)
        elif workbook_name and project_name:
            project = None
            for p in self.get_projects():
                if p.name == project_name:
                    project = p
                    break
            if not project:
                raise TableauConnectionError(f'Project does not exist: {project}')
            return Workbook(
                name=workbook_name,
                project_id=project.id,
                project_name=project.name
            )
        else:
            raise TableauConnectionError('Specify datasource_id or datasource_name and project_name')

    def __upload_in_chunks(self, file_path, file_size, chunk_size_mb=5):
        """ Uplaods a file to Tableau, in chunks.
            - PUT /api/api-version/sites/site-id/fileUploads
            - PUT /api/api-version/sites/site-id/fileUploads/upload_session_id

        Args:
            file_path (str):
            file_size (float): Overall size of the file in megabytes
            chunk_size_mb (int): The chunking size of increments to be uploaded

        Returns: An upload_session_id of the uploaded file
        """
        file_name = os.path.basename(file_path)
        # 1024 bytes in 1kb, 1000kb in 1mb
        chunk_size = 1024 * 1000 * chunk_size_mb
        # Initialize file upload session
        file = self.post(f'{self.url}/fileUploads')
        file = file['fileUpload']
        request_payload = RequestField(name='request_payload', data='')
        request_payload.make_multipart(content_type='text/xml')
        # Append file data in chunks
        with open(file_path, 'rb') as f:
            # Read in chunks
            for idx, chunk in enumerate(iter(lambda: f.read(chunk_size), b''), 1):
                post_body, content_type = self.__get_multipart_details([
                    ('request_payload', '', None, 'text/xml'),
                    ('tableau_file', chunk, file_name, 'application/octet-stream')
                ])
                self.put(
                    f'{self.url}/fileUploads/{file["uploadSessionId"]}',
                    data=post_body, headers={'Content-Type': content_type}
                )
                if idx * chunk_size_mb > round(file_size, 1):
                    print('Uploaded chunk: %smb of %smb' % (round(file_size, 1), round(file_size, 1)))
                else:
                    print('Uploaded chunk: %smb of %smb' % (idx * chunk_size_mb, round(file_size, 1)))
        return file["uploadSessionId"]

    def publish_datasource(self, file_path, datasource_id=None, datasource_name=None, project_name=None, **kw):
        """ Publishes a datasource to Tableau Online.
            One of the following MUST be provided:
              - datasource_id: If the datasource already exists
              - datasource_name AND project name: If this is a new datasource

        Args:
            file_path (str): The path to the datasource file (.tds or .tdsx)
            datasource_id (str): The ID of the datasource in Tableau Online
            datasource_name (str): The name of the Datasource
            project_name (str): The name of the Project in Tableau Online

        Keyword Args:
            overwrite (bool): True to overwrite the datasource, if it exists
            as_job (bool): True to kick this off as an async job in Tableau Online
            append (bool): True to append the data to the datasource in Tableau Online
            connection (dict): A dict of connection credentials to embed in the datasource
                i.e. 'username' and 'password'

        Returns: A Datasource Tableau server object
        """
        overwrite = kw.pop('overwrite', True)
        as_job = kw.pop('as_job', False)
        append = kw.pop('append', False)
        connection = kw.pop('connection', None)
        file_name = os.path.basename(file_path)
        # 1024 bytes in 1kb, 1000kb in 1mb
        file_size_mb = os.path.getsize(file_path) / 1024 / 1000
        extension = file_path.split('.')[-1]
        datasource = self.__get_datasource_for_publication(datasource_id, datasource_name, project_name)
        ds_xml = datasource.publish_xml(connection)
        # Datasource must be 64mb or less to publish all at once
        maximum_megabytes = 64
        if file_size_mb > maximum_megabytes:
            upload_session_id = self.__upload_in_chunks(file_path, file_size_mb, maximum_megabytes)
            publish_url = f'{self.url}/datasources?uploadSessionId={upload_session_id}' \
                          f'&datasourceType={extension}&overwrite={overwrite}&append={append}&asJob={as_job}'
            post_body, content_type = self.__get_multipart_details([
                ('request_payload', ds_xml, None, 'text/xml')
            ])
        else:
            publish_url = f'{self.url}/datasources?datasourceType={extension}' \
                          f'&overwrite={overwrite}&append={append}&asJob={as_job}'
            with open(file_path, 'rb') as f:
                post_body, content_type = self.__get_multipart_details([
                    ('request_payload', ds_xml, None, 'text/xml'),
                    ('tableau_datasource', f.read(), file_name, 'application/octet-stream')
                ])

        # Finally, publish the file uploaded
        content = self.post(publish_url, data=post_body, headers={'Content-Type': content_type})
        self.__transform_datasource(content['datasource'])
        return Datasource(**content['datasource'])

    def publish_workbook(self, file_path, workbook_id=None, workbook_name=None, project_name=None, **kw):
        """ Publishes a workbook to Tableau Online.
            One of the following MUST be provided:
              - workbook_id: If the workbook already exists
              - workbook_name AND project name: If this is a new workbook

        Args:
            file_path (str): The path to the Workbook file (.twb or .twbx)
            workbook_id (str): The ID of the Workbook in Tableau Online
            workbook_name (str): The name of the Workbook
            project_name (str): The name of the Project in Tableau Online

        Keyword Args:
            overwrite (bool): True to overwrite the datasource, if it exists
            as_job (bool): True to kick this off as an async job in Tableau Online
            skip_connection_check (bool): True for Tableau server to not check if,
                a non-published connection, of a workbook is reachable
            connections (list[dict]): A list of connections
                i.e. [{address, port, username, password}]

        Returns: A Workbook Tableau server object
        """
        overwrite = kw.pop('overwrite', True)
        as_job = kw.pop('as_job', False)
        skip_connection_check = kw.pop('skip_connection_check', False)
        connections = kw.pop('connections', None)
        file_name = os.path.basename(file_path)
        # 1024 bytes in 1kb, 1000kb in 1mb
        file_size_mb = os.path.getsize(file_path) / 1024 / 1000
        extension = file_path.split('.')[-1]
        workbook = self.__get_workbook_for_publication(workbook_id, workbook_name, project_name)
        wb_xml = workbook.publish_xml(connections)
        # Datasource must be 64mb or less to publish all at once
        maximum_megabytes = 64
        if file_size_mb > maximum_megabytes:
            upload_session_id = self.__upload_in_chunks(file_path, file_size_mb, maximum_megabytes)
            publish_url = f'{self.url}/workbooks?uploadSessionId={upload_session_id}' \
                          f'&workbookType={extension}' \
                          f'&skipConnectionCheck={skip_connection_check}' \
                          f'&overwrite={overwrite}&asJob={as_job}'
            post_body, content_type = self.__get_multipart_details([
                ('request_payload', wb_xml, None, 'text/xml')
            ])
        else:
            publish_url = f'{self.url}/workbooks?workbookType={extension}' \
                          f'&overwrite={overwrite}' \
                          f'&skipConnectionCheck={skip_connection_check}' \
                          f'&asJob={as_job}'
            with open(file_path, 'rb') as f:
                post_body, content_type = self.__get_multipart_details([
                    ('request_payload', wb_xml, None, 'text/xml'),
                    ('tableau_workbook', f.read(), file_name, 'application/octet-stream')
                ])

        # Finally, publish the file uploaded
        content = self.post(publish_url, data=post_body, headers={'Content-Type': content_type})
        self.__transform_workbook(content['workbook'])
        return Workbook(**content['workbook'])

    def refresh_datasource(self, datasource_id):
        content = self.post(f'{self.url}/datasources/{datasource_id}/refresh', json={})
        self.__transform_job(content['job'])
        return Job(**content['job'])

    def refresh_workbook(self, workbook_id):
        content = self.post(f'{self.url}/workbooks/{workbook_id}/refresh', json={})
        self.__transform_job(content['job'])
        return Job(**content['job'])

    def update_datasource_connection(self, datasource_id, connection: Connection):
        content = self.put(
            f'{self.url}/datasources/{datasource_id}/connections/{connection.id}',
            json={'connection': connection.to_dict()}
        )
        self.__transform_job(content['job'])
        return Job(**content['job'])

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

        for c in self.get_datasource_connections(datasource_id):
            if c.type.lower() == connection_type.lower():
                c.userName = credentials['username']
                c.password = credentials['password']
                c.embedPassword = True
                self.update_datasource_connection(datasource_id, c)


if __name__ == '__main__':
    load_dotenv()
    ts = TableauServer(
        user=os.getenv('TABLEAU_USER'),
        password=os.getenv('TABLEAU_PW'),
        host=os.getenv('TABLEAU_HOST'),
        site=os.getenv('TABLEAU_SITE'),
        api_version=float(os.getenv('TABLEAU_API'))
    )
    ds_id = 'bd490c6c-7f61-4214-bcd2-05d3787d7f47'
    downloads = 'downloads'
    path = ts.download_datasource(datasource_id=ds_id, file_dir=downloads, include_extract=True)
    print(path)
    source = ts.publish_datasource(
        datasource_id=ds_id,
        file_path=path,
        connection={
            'username': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PW')
        }
    )
    print(source)
    job = ts.refresh_datasource(datasource_id=ds_id)
    print(job)

