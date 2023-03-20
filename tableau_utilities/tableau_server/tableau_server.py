import logging
import requests
import cgi
import os
import json
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata

import tableau_utilities.tableau_server.tableau_server_objects as tso
from tableau_utilities.general.funcs import flatten_dict


class TableauConnectionError(Exception):
    """ An Exception in the TableauServer connection """
    pass


class TableauServer:
    """ Connects and interacts with Tableau Online/Server, via the REST API. """

    def __init__(self, host, site, user=None, password=None, personal_access_token_secret=None, personal_access_token_name=None, api_version=None):
        """ To sign in to Tableau a user needs either a username & password or token secret & token name

        Args:
            host (str): Tableau server address
            user (str): The username to sign in to Tableau Online with
            password (str): The password to sign in to Tableau Online with
            personal_access_token_name (str): The name of the personal access token used
            personal_access_token_secret (str): The secret of the personal access token used
            site (str): The Tableau Online site id
            api_version (float): The Tableau REST API version
        """
        self.user = user
        self.__pw = password
        self.personal_access_token_secret = personal_access_token_secret
        self.personal_access_token_name = personal_access_token_name
        self.host = host
        self.site = site
        self.api = api_version or 3.18
        # Set by class
        self._auth_token = None
        self.url = None
        # Create a session on initialization
        self.session = requests.session()
        self.session.headers.update({'accept': 'application/json', 'content-type': 'application/json'})
        # Sign in on initialization
        self.__sign_in()

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

        if self.personal_access_token_secret and self.personal_access_token_name:
            body = {"credentials": {"personalAccessTokenSecret": self.personal_access_token_secret,
                                    "personalAccessTokenName": self.personal_access_token_name,
                                    "site": {"contentUrl": self.site}}}
        elif self.user and self.__pw:
            body = {"credentials": {"name": self.user, "password": self.__pw, "site": {"contentUrl": self.site}}}
        else:
            raise TableauConnectionError(
                'Please provide either user and password, or token_secret and token_name'
            )

        res = self.post(url, json=body).get('credentials', {})
        # Set auth token and site ID attributes on sign in
        self.session.headers.update({'x-tableau-auth': res.get('token')})
        self.site = res.get('site', {}).get('id')
        self.url = f"{self.host}/api/{self.api}/sites/{self.site}"

    def sign_out(self):
        """ Destroys the active session and invalidates authentication token. """
        self.post(url=f"{self.host}/api/{self.api}/auth/signout")
        self.session.close()

    @staticmethod
    def __validate_response(response):
        """ Validates the response received from an API call

        Args:
            response (requests.Response): A requests Response object
            raise_for_status (bool): True to raise an error on a bad response

        Returns: The response content as a JSON dict
        """
        info = response.json()
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

        res = self.session.get(url, headers=headers, **params)
        return self.__validate_response(res)

    def delete(self, url, headers=None, **params):
        """ DELETE request for the Tableau REST API

        Args:
            url (str): URL endpoint for DELETE call
            headers (dict): DELETE call header

        Returns: The response content as a JSON dict
        """
        res = self.session.delete(url, headers=headers, **params)
        return self.__validate_response(res)

    def post(self, url, json=None, headers=None, **params):
        """ POST request for the Tableau REST API

        Args:
            url (str): URL endpoint for POST call
            json (dict): The POST call JSON payload
            headers (dict): POST call header

        Returns: The response content as a JSON dict
        """
        res = self.session.post(url, json=json, headers=headers, **params)
        return self.__validate_response(res)

    def put(self, url, json=None, headers=None, **params):
        """ PUT request for the Tableau REST API

        Args:
            url (str): URL endpoint for PUT call
            json (dict): The PUT call JSON payload
            headers (dict): PUT call header

        Returns: The response content as a JSON dict
        """
        res = self.session.put(url, json=json, headers=headers, **params)
        return self.__validate_response(res)

    @staticmethod
    def __transform_tableau_object(object_dict):
        """ Transforms the object dict from a Tableau REST API call
        Args:
            object_dict (dict): The object dict from a Tableau REST API call
        """
        if object_dict.get('tags'):
            object_dict['tags'] = [t['label'] for t in object_dict['tags']['tag']]
        update = dict()
        flatten_dict(object_dict, update)
        object_dict.clear()
        object_dict.update(update)

    def __get_objects_pager(self, url, obj, page_size=100):
        """ GET all objects in the site
        Args:
            url: The url of for the request, i.e /api/api-version/sites/site-id/groups
            obj: The name of the object being requested
            page_size: The size of the page (number of objects per page)
        Returns: A list of the objects
        """
        response = self.get(url)
        total_available = response.get('pagination', {}).get('totalAvailable', page_size)
        total_available = int(total_available)
        current = 0
        page = 0
        while current < total_available:
            current += page_size
            page += 1
            if '?' in url:
                page_url = f'{url}&pageSize={page_size}&pageNumber={page}'
            else:
                page_url = f'{url}?pageSize={page_size}&pageNumber={page}'
            logging.info('GET %s --> %s/%s', page_url, current, total_available)
            res = self.get(page_url)
            for obj_dict in res[f'{obj}s'][obj]:
                self.__transform_tableau_object(obj_dict)
                yield obj_dict

    def get_datasource(self, datasource_id=None, datasource_name=None, datasource_project=None):
        """ Queries for a datasource in the site
            URI GET /api/api-version/sites/site-id/datasources/datasource_id

            (Optional) Can get the datasource either by ID, or by name & project.

        Args:
              datasource_id (str): The ID of the datasource
              datasource_name (str): The name of the datasource
              datasource_project (str): The name of the project the datasource is in

        Returns: A Datasource Tableau object
        """
        if datasource_id:
            d = self.get(f'{self.url}/datasources/{datasource_id}')
            d = d['datasource']
            self.__transform_tableau_object(d)
            return tso.Datasource(**d)
        elif datasource_name and datasource_project:
            for d in self.get_datasources():
                if d.name == datasource_name and d.project_name == datasource_project:
                    return d
            raise TableauConnectionError(
                f'Datasource not found:\n\tName    -> {datasource_name}\n\tProject -> {datasource_project}'
            )

        raise TableauConnectionError(
            'Please provide either the datasource_id, or both datasource_name and datasource_project'
        )

    def get_datasources(self):
        """ Queries for all datasources in the site
            URI GET /api/api-version/sites/site-id/datasources
        Returns: All datasources in the site
        """
        url = f"{self.url}/datasources?fields=_default_" \
              f",favoritesTotal" \
              f",databaseName" \
              f",connectedWorkbooksCount" \
              f",hasAlert" \
              f",hasExtracts" \
              f",isPublished" \
              f",serverName"

        for d in self.__get_objects_pager(url, 'datasource'):
            yield tso.Datasource(**d)

    def get_datasource_connections(self, datasource_id):
        """ Queries for all Connection Tableau objects in the datasource
            URI GET /api/api-version/sites/site-id/datasources/datasource_id/connections
        Args:
            datasource_id (str): The ID of the Tableau Datasource
        Returns: All Connection Tableau objects in the datasource
        """
        url = f'{self.url}/datasources/{datasource_id}/connections'
        for connection in self.__get_objects_pager(url, 'connection'):
            yield tso.Connection(**connection)

    def get_workbook_connections(self, workbook_id):
        """ Queries for all Connection Tableau objects in the workbook
            URI GET /api/api-version/sites/site-id/workbooks/workbook_id/connections
        Args:
            workbook_id (str): The ID of the Tableau Workbook
        Returns: All Connection Tableau objects in the workbook
        """
        url = f'{self.url}/workbooks/{workbook_id}/connections'
        for connection in self.__get_objects_pager(url, 'connection'):
            yield tso.Connection(**connection)

    def get_workbook(self, workbook_id=None, workbook_name=None, workbook_project=None):
        """ Queries for a workbook in the site
            URI GET /api/api-version/sites/site-id/workbooks/workbook_id
        Args:
            workbook_id (str): The ID of the Tableau Workbook
            workbook_name (str): The name of the workbook
            workbook_project (str): The name of the project the workbook is in
        Returns: A Workbooks Tableau object
        """
        if workbook_id:
            w = self.get(f'{self.url}/workbooks/{workbook_id}')
            w = w['workbook']
            self.__transform_tableau_object(w)
            return tso.Workbook(**w)
        elif workbook_name and workbook_project:
            for w in self.get_workbooks():
                if w.name == workbook_name and w.project_name == workbook_project:
                    return w
            raise TableauConnectionError(
                f'Datasource not found:\n\tName    -> {workbook_name}\n\tProject -> {workbook_project}'
            )

        raise TableauConnectionError(
            'Please provide either the workbook_id, or both workbook_name and workbook_project'
        )

    def get_workbooks(self):
        """ Queries for all workbooks in the site
            URI GET /api/api-version/sites/site-id/workbooks
        Returns: All workbooks in the site
        """
        for w in self.__get_objects_pager(f"{self.url}/workbooks", 'workbook'):
            yield tso.Workbook(**w)

    def get_view(self, view_id):
        """ Queries for a view in the site
            URI GET /api/api-version/sites/site-id/views/view_id
        Args:
            view_id (str): The ID of the Tableau View
        Returns: A View Tableau object
        """
        v = self.get(f'{self.url}/views/{view_id}')
        v = v['view']
        return tso.View(**v)

    def get_views(self):
        """ Queries for all views in the site
            URI GET /api/api-version/sites/site-id/views
        Returns: All workbooks in the site
        """
        url = f"{self.url}/views?fields=_default_,sheetType,usage"
        for v in self.__get_objects_pager(url, 'view'):
            yield tso.View(**v)

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
        url = f"{self.url}/projects?fields=_default_" \
              f",topLevelProject" \
              f",writeable" \
              f",contentsCounts.projectCount" \
              f",contentsCounts.viewCount" \
              f",contentsCounts.datasourceCount" \
              f",contentsCounts.workbookCount"
        for p in self.__get_objects_pager(url, 'project'):
            project = tso.Project(**p)
            # Only get Top Level projects
            if top_level_only and not project.top_level_project:
                continue
            yield project

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
        url = f"{self.url}/groups?fields=_default_,userCount,minimumSiteRole"
        for g in self.__get_objects_pager(url, 'group'):
            yield tso.Group(**g)

    def get_user(self, user_id):
        """ Queries for the user by user_id
            URI GET /api/api-version/sites/site-id/users/user_id
        Args:
            user_id (str): The ID of the user in Tableau Online
        Returns: A Tableau User object specified by ID
        """
        u = self.get(f"{self.url}/users/{user_id}")
        u = u['user']
        self.__transform_tableau_object(u)
        return tso.User(**u)

    def get_users(self):
        """ Queries for all users in the site
            URI GET /api/api-version/sites/site-id/users
        Returns: All users in the site
        """
        url = f"{self.url}/users?fields=_default_,fullName,email"
        for user in self.__get_objects_pager(url, 'user'):
            yield tso.User(**user)

    def get_user_groups(self):
        """ Queries for all groups and all user in those groups
            URI GET /api/api-version/sites/site-id/groups/group_id/users
        Returns: A list of all user/group combinations
        """
        for group in self.get_groups():
            for u in self.__get_objects_pager(f"{self.url}/groups/{group.id}/users", 'user'):
                yield group, tso.User(**u)

    def create_project(self, name, description='', content_permissions='LockedToProject'):
        """ Creates a project.

        Args:
            name (str): The name of the project
            description (str): The description of the project
            content_permissions (str): The content permissions, e.g. LockedToProject
        """
        self.post(
            f'{self.url}/projects',
            {
                'project': {
                    'name': name,
                    'description': description,
                    'contentPermissions': content_permissions
                }
            }
        )

    def create_group(self, name, minimum_site_role='Viewer'):
        """ Creates a group.

        Args:
            name (str): The name of the Group
            minimum_site_role (str): The minimum site role of the group, e.g. Viewer
        """
        self.post(
            f'{self.url}/groups',
            {
                'group': {
                    'name': name,
                    'minimumSiteRole': minimum_site_role
                }
            }
        )

    def __download_object(self, url, file_dir=None):
        """ Downloads a datasource from Tableau Online

        Args:
            url (str): The URL for the request
            file_dir (str): The file directory to write the file to

        Returns: The absolute path to the file
        """
        res = self.session.get(url, stream=True)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise TableauConnectionError(err) from err
        _, params = cgi.parse_header(res.headers["Content-Disposition"])
        file_name = os.path.basename(params["filename"])
        if file_dir:
            os.makedirs(file_dir, exist_ok=True)
            path = os.path.join(file_dir, file_name)
        else:
            path = file_name
        with open(path, "wb") as f:
            # Download in 1024 bytes (1kb) chunks
            for chunk in res.iter_content(1024):
                f.write(chunk)
        return os.path.abspath(path)

    def download_datasource(self, datasource_id, file_dir=None, include_extract=False):
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

    def download_workbook(self, workbook_id, file_dir=None, include_extract=False):
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
            return tso.Datasource(
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
            return tso.Workbook(
                name=workbook_name,
                project_id=project.id,
                project_name=project.name
            )
        else:
            raise TableauConnectionError('Specify datasource_id or datasource_name and project_name')

    def __upload_in_chunks(self, file_path, chunk_size_mb=5):
        """ Uplaods a file to Tableau, in chunks.
            - PUT /api/api-version/sites/site-id/fileUploads
            - PUT /api/api-version/sites/site-id/fileUploads/upload_session_id

        Args:
            file_path (str): The path to the file
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
            upload_session_id = self.__upload_in_chunks(file_path, maximum_megabytes)
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
        self.__transform_tableau_object(content['datasource'])
        return tso.Datasource(**content['datasource'])

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
            upload_session_id = self.__upload_in_chunks(file_path, maximum_megabytes)
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
        self.__transform_tableau_object(content['workbook'])
        return tso.Workbook(**content['workbook'])

    def refresh_datasource(self, datasource_id):
        content = self.post(f'{self.url}/datasources/{datasource_id}/refresh', json={})
        self.__transform_tableau_object(content['job'])
        return tso.Job(**content['job'])

    def refresh_workbook(self, workbook_id):
        content = self.post(f'{self.url}/workbooks/{workbook_id}/refresh', json={})
        self.__transform_tableau_object(content['job'])
        return tso.Job(**content['job'])

    def update_datasource_connection(self, datasource_id, connection: tso.Connection):
        conn_dict = connection.dict()
        if 'user_name' in conn_dict:
            conn_dict.setdefault('userName', conn_dict.pop('user_name'))
        if 'embed_password' in conn_dict:
            conn_dict.setdefault('embedPassword', conn_dict.pop('embed_password'))
        if 'password' in conn_dict:
            conn_dict.setdefault('password', conn_dict.pop('password'))
        if 'server_address' in conn_dict:
            conn_dict.setdefault('serverAddress', conn_dict.pop('server_address'))
        if 'query_tagging_enabled' in conn_dict:
            conn_dict.setdefault('queryTaggingEnabled', conn_dict.pop('query_tagging_enabled'))

        content = self.put(
            f'{self.url}/datasources/{datasource_id}/connections/{connection.id}',
            json={'connection': conn_dict}
        )

        self.__transform_tableau_object(content['connection'])
        return tso.Connection(**content['connection'])

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
                c.user_name = credentials['username']
                c.password = credentials['password']
                c.embed_password = True
                response = self.update_datasource_connection(datasource_id, c)
                return response
