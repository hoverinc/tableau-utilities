import logging
import tableau_utilities.tableau_server.tableau_server_objects as tso
from tableau_utilities.tableau_server.static import TableauConnectionError, transform_tableau_object
from tableau_utilities.tableau_server.base import Base


class Get(Base):
    """ Core Get functionality of the TableauServer class """
    def __init__(self, parent):
        super().__init__(parent)

    def __get_objects_pager(self, url, obj, page_size=100):
        """ GET all objects in the site
        Args:
            url: The url of for the request, i.e /api/api-version/sites/site-id/groups
            obj: The name of the object being requested
            page_size: The size of the page (number of objects per page)
        Returns: A list of the objects
        """
        response = self._get(url)
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
            res = self._get(page_url)
            for obj_dict in res[f'{obj}s'][obj]:
                transform_tableau_object(obj_dict)
                yield obj_dict

    def datasources(self):
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

    def datasource(self, datasource_id=None, datasource_name=None, datasource_project=None):
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
            d = self._get(f'{self.url}/datasources/{datasource_id}')
            d = d['datasource']
            transform_tableau_object(d)
            return tso.Datasource(**d)
        elif datasource_name and datasource_project:
            for d in self.datasources():
                if d.name == datasource_name and d.project_name == datasource_project:
                    return d
            raise TableauConnectionError(
                f'Datasource not found:\n\tName    -> {datasource_name}\n\tProject -> {datasource_project}'
            )

        raise TableauConnectionError(
            'Please provide either the datasource_id, or both datasource_name and datasource_project'
        )

    def datasource_connections(self, datasource_id):
        """ Queries for all Connection Tableau objects in the datasource
            URI GET /api/api-version/sites/site-id/datasources/datasource_id/connections
        Args:
            datasource_id (str): The ID of the Tableau Datasource
        Returns: All Connection Tableau objects in the datasource
        """
        url = f'{self.url}/datasources/{datasource_id}/connections'
        for connection in self.__get_objects_pager(url, 'connection'):
            yield tso.Connection(**connection)

    def workbook_connections(self, workbook_id):
        """ Queries for all Connection Tableau objects in the workbook
            URI GET /api/api-version/sites/site-id/workbooks/workbook_id/connections
        Args:
            workbook_id (str): The ID of the Tableau Workbook
        Returns: All Connection Tableau objects in the workbook
        """
        url = f'{self.url}/workbooks/{workbook_id}/connections'
        for connection in self.__get_objects_pager(url, 'connection'):
            yield tso.Connection(**connection)

    def workbooks(self):
        """ Queries for all workbooks in the site
            URI GET /api/api-version/sites/site-id/workbooks
        Returns: All workbooks in the site
        """
        for w in self.__get_objects_pager(f"{self.url}/workbooks", 'workbook'):
            yield tso.Workbook(**w)

    def workbook(self, workbook_id=None, workbook_name=None, workbook_project=None):
        """ Queries for a workbook in the site
            URI GET /api/api-version/sites/site-id/workbooks/workbook_id
        Args:
            workbook_id (str): The ID of the Tableau Workbook
            workbook_name (str): The name of the workbook
            workbook_project (str): The name of the project the workbook is in
        Returns: A Workbooks Tableau object
        """
        if workbook_id:
            w = self._get(f'{self.url}/workbooks/{workbook_id}')
            w = w['workbook']
            transform_tableau_object(w)
            return tso.Workbook(**w)
        elif workbook_name and workbook_project:
            for w in self.workbooks():
                if w.name == workbook_name and w.project_name == workbook_project:
                    return w
            raise TableauConnectionError(
                f'Datasource not found:\n\tName    -> {workbook_name}\n\tProject -> {workbook_project}'
            )

        raise TableauConnectionError(
            'Please provide either the workbook_id, or both workbook_name and workbook_project'
        )

    def views(self):
        """ Queries for all views in the site
            URI GET /api/api-version/sites/site-id/views
        Returns: All workbooks in the site
        """
        url = f"{self.url}/views?fields=_default_,sheetType,usage"
        for v in self.__get_objects_pager(url, 'view'):
            yield tso.View(**v)

    def view(self, view_id):
        """ Queries for a view in the site
            URI GET /api/api-version/sites/site-id/views/view_id
        Args:
            view_id (str): The ID of the Tableau View
        Returns: A View Tableau object
        """
        v = self._get(f'{self.url}/views/{view_id}')
        v = v['view']
        return tso.View(**v)

    def projects(self, top_level_only=True):
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

    def project(self, project_id):
        """ Queries for the project by project_id
            URI GET /api/api-version/sites/site-id/projects/project_id
        Args:
            project_id (str): The ID of the project in Tableau Online
        Returns: A Tableau Project object specified by ID
        """
        for p in self.projects():
            if p.id == project_id:
                return p

    def groups(self):
        """ Queries for all groups in the site
            URI GET /api/api-version/sites/site-id/groups
        Returns: All groups in the site
        """
        url = f"{self.url}/groups?fields=_default_,userCount,minimumSiteRole"
        for g in self.__get_objects_pager(url, 'group'):
            yield tso.Group(**g)

    def group(self, group_id):
        """ Queries for the group by user_id
            URI GET /api/api-version/sites/site-id/groups/group_id
        Args:
            group_id (str): The ID of the group in Tableau Online
        Returns: A Tableau Group object specified by ID
        """
        for group in self.groups():
            if group.id == group_id:
                return group

    def users(self):
        """ Queries for all users in the site
            URI GET /api/api-version/sites/site-id/users
        Returns: All users in the site
        """
        url = f"{self.url}/users?fields=_default_,fullName,email"
        for user in self.__get_objects_pager(url, 'user'):
            yield tso.User(**user)

    def user(self, user_id):
        """ Queries for the user by user_id
            URI GET /api/api-version/sites/site-id/users/user_id
        Args:
            user_id (str): The ID of the user in Tableau Online
        Returns: A Tableau User object specified by ID
        """
        u = self._get(f"{self.url}/users/{user_id}")
        u = u['user']
        transform_tableau_object(u)
        return tso.User(**u)

    def user_groups(self):
        """ Queries for all groups and all user in those groups
            URI GET /api/api-version/sites/site-id/groups/group_id/users
        Returns: A list of all user/group combinations
        """
        for group in self.groups():
            for u in self.__get_objects_pager(f"{self.url}/groups/{group.id}/users", 'user'):
                yield group, tso.User(**u)
