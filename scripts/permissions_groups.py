import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import sys
import getpass
import requests # Contains methods used to make HTTP requests
import re
import permissions_sets as PS
from collections import defaultdict
from settings import tableau_credentials, VERSION
# from urllib2 import Request, urlopen

# The namespace for the REST API is 'http://tableausoftware.com/api' for Tableau Server 9.0
# or 'http://tableau.com/api' for Tableau Server 9.1 or later
XMLNS = {'t': 'http://tableau.com/api'}


class ApiCallError(Exception):
    """ ApiCallError """
    pass


class GroupProjectPermissions:
    """ Enforces permissions for Tableau Groups and Projects """

    def __init__(self, server, auth_token, site_id):
        """
        Args:
            server (str): The Tableau server URL
            auth_token (str): The Tableau auth token
            site_id (str): The Tableau site ID
        """
        self.server = server
        self.site_id = site_id
        self.auth_token = auth_token

    @staticmethod
    def _encode_for_display(text):
        """ Encodes strings so they can display as ASCII in a Windows terminal window.
            This function also encodes strings for processing by xml.etree.ElementTree functions.
            Unicode characters are converted to ASCII placeholders (for example, "?").

        Args:
            text (str): A string of text

        Returns: An ASCII-encoded version of the text
        """
        return text.encode('ascii', errors="backslashreplace").decode('utf-8')

    @staticmethod
    def _check_status(server_response, success_code):
        """ Checks the server response for possible errors.
            Throws an ApiCallError exception if the API call fails.

        Args:
            server_response: The response received from the server
            success_code: The expected success code for the response
        """
        if server_response.status_code != success_code:
            parsed_response = ET.fromstring(server_response.text)

            # Obtain the 3 xml tags from the response: error, summary, and detail tags
            error_element = parsed_response.find('t:error', namespaces=XMLNS)
            summary_element = parsed_response.find('.//t:summary', namespaces=XMLNS)
            detail_element = parsed_response.find('.//t:detail', namespaces=XMLNS)

            # Retrieve the error code, summary, and detail if the response contains them
            code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
            summary = summary_element.text if summary_element is not None else 'unknown summary'
            detail = detail_element.text if detail_element is not None else 'unknown detail'
            error_message = '{0}: {1} - {2}'.format(code, summary, detail)
            raise ApiCallError(error_message)
        return

    def lock_all_permissions_to_the_project(self, project_list):
        """ Lock or verify lock to default permissions for each project

        Args:
            project_list (list): A list of projects
        """

        for project in project_list:
            project_id = project.get('id')
            project_name = project.get('name')
            project_description = project.get('description')
            project_contentPermissions = project.get('contentPermissions')

            # print("PROJECT:", project_name, project_id)

            if project_contentPermissions == 'ManagedByOwner':
                print("Locking For:", project_name)
                print("...trying to lock...")
                self.set_project_to_lock(project_id)
                # self.set_project_to_lock(self.server, self.auth_token, self.site_id, project_id)
            else:
                print("Lock Confirmed For:", project_name)

    def set_project_to_lock(self, project_id):
        """ Lock or verify lock to default permissions for the project

        Args:
            project_id (str): ID of the Tableau project
        """
        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}"

        # Build the request
        xml_request = ET.Element('tsRequest')
        permissions_element = ET.SubElement(xml_request, 'project', contentPermissions='LockedToProject')
        # ET.SubElement(permissions_element, 'project', contentPermissions='LockedToProject')
        # ET.SubElement(permissions_element, 'LockedToProject')
        xml_request = ET.tostring(xml_request)

        server_request = requests.put(url, data=xml_request, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_request, 200)
        return

    def query_permissions_for_group_project(self, project_id, group_id):
        """ Queries and returns the permissions for a single project and group
            GET api/api-version/sites/site-id/projects/project-id/permissions

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group

        Returns (dict): A dict of permissions
        """
        permission_mapping = {
            'Read': 'Project_Read_View',
            'Write': 'Project_Write',
            'ProjectLeader': 'Project_ProjectLeader'
        }

        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}/permissions"
        server_response = requests.get(url, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_response, 200)
        server_response = self._encode_for_display(server_response.text)

        # Reads and parses the response
        parsed_response = ET.fromstring(server_response)

        project_permissions = dict()
        # Find all the capabilities for a specific user
        capabilities = parsed_response.findall('.//t:granteeCapabilities', namespaces=XMLNS)
        for capability in capabilities:
            group = capability.find('.//t:group', namespaces=XMLNS)
            if group is not None and group.get('id') == group_id:
                project_permissions = capability.findall('.//t:capability', namespaces=XMLNS)
                project_permissions = {p.get('name'): p.get('mode') for p in project_permissions}

        existing_permissions = dict()
        for name, v in permission_mapping.items():
            existing_permissions[v] = project_permissions.get(name)

        return existing_permissions

    def query_permissions_for_group_project_default_workbook(self, project_id, group_id):
        """ Queries and returns the default-permissions for workbooks for a single project and group
            GET api/api-version/sites/site-id/projects/project-id/permissions

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group

        Returns (dict): A dict of permissions
        """
        permission_mapping = {
            'Read': 'Workbook_Read_View',
            'ExportImage': 'Workbook_ExportImage',
            'ExportData': 'Workbook_ExportData',
            'ViewComments': 'Workbook_ViewComments',
            'AddComment': 'Workbook_AddComment',
            'Filter': 'Workbook_Filter',
            'ViewUnderlyingData': 'Workbook_ViewUnderlyingData',
            'ShareView': 'Workbook_ShareView',
            'WebAuthoring': 'Workbook_WebAuthoring',
            'Write': 'Workbook_Write',
            'ExportXml': 'Workbook_ExportXml',
            'ChangeHierarchy': 'Workbook_ChangeHierarchy',
            'Delete': 'Workbook_Delete',
            'ChangePermissions': 'Workbook_ChangePermissions'
        }

        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}/default-permissions/workbooks"
        server_response = requests.get(url, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_response, 200)
        server_response = self._encode_for_display(server_response.text)

        # Reads and parses the response
        parsed_response = ET.fromstring(server_response)

        project_permissions = {}

        # Find all the capabilities for a specific user
        capabilities = parsed_response.findall('.//t:granteeCapabilities', namespaces=XMLNS)
        for capability in capabilities:
            group = capability.find('.//t:group', namespaces=XMLNS)
            if group is not None and group.get('id') == group_id:
                project_permissions = capability.findall('.//t:capability', namespaces=XMLNS)
                project_permissions = {p.get('name'): p.get('mode') for p in project_permissions}

        existing_permissions = {}
        for name, v in permission_mapping.items():
            existing_permissions[v] = project_permissions.get(name)

        return existing_permissions

    def query_permissions_for_group_project_default_datasource(self, project_id, group_id):
        """ Queries and returns the default-permissions for datasources for a single project and group
            GET api/api-version/sites/site-id/projects/project-id/permissions

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group

        Returns (dict): A dict of permissions
        """
        permission_mapping = {
            'Read': 'Datasource_Read_View',
            'Connect': 'Datasource_Connect',
            'Write': 'Datasource_Write',
            'ExportXml': 'Datasource_ExportXml',
            'Delete': 'Datasource_Delete',
            'ChangePermissions': 'Datasource_ChangePermissions'
        }

        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}/default-permissions/datasources"
        server_response = requests.get(url, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_response, 200)
        server_response = self._encode_for_display(server_response.text)

        # Reads and parses the response
        parsed_response = ET.fromstring(server_response)

        project_permissions = dict()
        # Find all the capabilities for a specific user
        capabilities = parsed_response.findall('.//t:granteeCapabilities', namespaces=XMLNS)
        for capability in capabilities:
            group = capability.find('.//t:group', namespaces=XMLNS)
            if group is not None and group.get('id') == group_id:
                project_permissions = capability.findall('.//t:capability', namespaces=XMLNS)
                project_permissions = {p.get('name'): p.get('mode') for p in project_permissions}

        existing_permissions = dict()
        for name, v in permission_mapping.items():
            existing_permissions[v] = project_permissions.get(name)

        # print(existing_permissions)
        return existing_permissions

    def add_permissions_for_group_project(self, project_id, group_id, permission_set):
        """ Adds the specified permission to the project for the desired group.

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group
            permission_set (dict): A dict of permissions from permissions_sets
        """
        # project Permissions
        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}/permissions"
        # Build the request
        xml_request = ET.Element('tsRequest')
        permissions_element = ET.SubElement(xml_request, 'permissions')
        # ET.SubElement(permissions_element, 'workbook', id=workbook_id)
        grantee_element = ET.SubElement(permissions_element, 'granteeCapabilities')
        ET.SubElement(grantee_element, 'group', id=group_id)
        capabilities_element = ET.SubElement(grantee_element, 'capabilities')

        for k, v in permission_set.items():
            capability_name = re.findall('^[^_]+_([^_]+)', k)[0]
            ET.SubElement(capabilities_element, 'capability', name=capability_name, mode=v)

        xml_request = ET.tostring(xml_request)
        print(xml_request)

        server_request = requests.put(url, data=xml_request, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_request, 200)
        print("\tSuccessfully added/updated permission project permissions")
        return

    def add_permissions_for_group_default_workbook(self, project_id, group_id, permission_set):
        """ Adds the specified default-permissions to the workbooks of a project for the desired group

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group
            permission_set (dict): A dict of permissions from permissions_sets
        """
        # project Permissions
        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}/default-permissions/workbooks"
        # Build the request
        xml_request = ET.Element('tsRequest')
        permissions_element = ET.SubElement(xml_request, 'permissions')
        # ET.SubElement(permissions_element, 'workbook', id=workbook_id)
        grantee_element = ET.SubElement(permissions_element, 'granteeCapabilities')
        ET.SubElement(grantee_element, 'group', id=group_id)
        capabilities_element = ET.SubElement(grantee_element, 'capabilities')

        for k, v in permission_set.items():
            capability_name =re.findall('^[^_]+_([^_]+)', k)[0]
            ET.SubElement(capabilities_element, 'capability', name=capability_name, mode=v)

        xml_request = ET.tostring(xml_request)
        server_request = requests.put(url, data=xml_request, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_request, 200)
        print("\tSuccessfully added/updated permission project workbook")
        return

    def add_permissions_for_group_default_datasource(self, project_id, group_id, permission_set):
        """ Adds the specified default-permissions to the datasources of a project for the desired group

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group
            permission_set (dict): A dict of permissions from permissions_sets
        """
        # project Permissions
        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}/default-permissions/datasources"
        # Build the request
        xml_request = ET.Element('tsRequest')
        permissions_element = ET.SubElement(xml_request, 'permissions')
        # ET.SubElement(permissions_element, 'workbook', id=workbook_id)
        grantee_element = ET.SubElement(permissions_element, 'granteeCapabilities')
        ET.SubElement(grantee_element, 'group', id=group_id)
        capabilities_element = ET.SubElement(grantee_element, 'capabilities')

        for k, v in permission_set.items():
            capability_name = re.findall('^[^_]+_([^_]+)', k)[0]
            ET.SubElement(capabilities_element, 'capability', name=capability_name, mode=v)

        xml_request = ET.tostring(xml_request)
        server_request = requests.put(url, data=xml_request, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_request, 200)
        print("\tSuccessfully added/updated permission project datasource")
        return

    def delete_permissions_for_group_project(self, project_id, group_id, capability_name, existing_capability_mode):
        """ Deletes a specific permission from the project for the group

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group
            capability_name (str): Name of the permission
            existing_capability_mode (str): The existing mode of the permission; Allow, or Deny
        """

        # project Permissions
        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}"
        url += f"/permissions/groups/{group_id}/{capability_name}/{existing_capability_mode}"
        print("\tDeleting existing permission")
        server_response = requests.delete(url, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_response, 204)
        return

    def delete_permissions_for_group_default_workbook(self, project_id, group_id, capability_name, existing_capability_mode):
        """ Deletes a specific permission from workbooks in the project for the group

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group
            capability_name (str): Name of the permission
            existing_capability_mode (str): The existing mode of the permission; Allow, or Deny
        """

        # project Permissions
        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}"
        url += f"/default-permissions/workbooks/groups/{group_id}/{capability_name}/{existing_capability_mode}"
        print("\tDeleting existing permission")
        server_response = requests.delete(url, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_response, 204)
        return

    def delete_permissions_for_group_default_datasource(self, project_id, group_id, capability_name, existing_capability_mode):
        """ Deletes a specific permission from datasources in the project for the group

        Args:
            project_id (str): ID of the Tableau project
            group_id (str): ID of the Tableau group
            capability_name (str): Name of the permission
            existing_capability_mode (str): The existing mode of the permission; Allow, or Deny
        """

        # project Permissions
        url = f"{self.server}/api/{VERSION}/sites/{self.site_id}/projects/{project_id}"
        url += f"/default-permissions/datasources/groups/{group_id}/{capability_name}/{existing_capability_mode}"
        print("\tDeleting existing permission")
        server_response = requests.delete(url, headers={'x-tableau-auth': self.auth_token})
        self._check_status(server_response, 204)
        return

    @staticmethod
    def combine_dicts(dicts):
        """
        Args:
            dicts (list): A list of dicts

        Returns: A dicts of all key value pairs from all listed dicts
        """
        super_dict = dict()
        for d in dicts:
            super_dict.update(d)
        return super_dict

    @staticmethod
    def dict_not_intersect(dict_a, dict_b):
        """
        Args:
            dict_a (dict): The first dict to compare to the second
            dict_b (dict): The second dict to compare to the first

        Returns: A dict of key value pairs from the first list that are not in the second
        """
        return {
            key_a: value_a
            for (key_a, value_a) in dict_a.items()
            if value_a and (key_a, value_a) not in dict_b.items()
        }

    def get_existing_permissions_for_project_group(self, project_id, group_id):
        project = self.query_permissions_for_group_project(project_id, group_id)
        workbook = self.query_permissions_for_group_project_default_workbook(project_id, group_id)
        datasource = self.query_permissions_for_group_project_default_datasource(project_id, group_id)

        existing_permission_set = self.combine_dicts([project, workbook, datasource])
        return existing_permission_set

    def delete_permission_set(self, project_id, group_id, permission_set):
        for k, v in permission_set.items():
            print(k, v)
            capability_name = re.findall('^[^_]+_([^_]+)', k)[0]
            if k.startswith('Project'):
                self.delete_permissions_for_group_project(project_id, group_id, capability_name, existing_capability_mode=v)
            elif k.startswith('Workbook'):
                self.delete_permissions_for_group_default_workbook(project_id, group_id, capability_name, existing_capability_mode=v)
            elif k.startswith('Datasource'):
                self.delete_permissions_for_group_default_datasource(project_id, group_id, capability_name, existing_capability_mode=v)

    def add_permission_set(self, project_id, group_id, permission_set):

        project_permission_set = {k: v for (k, v) in permission_set.items() if k.startswith('Project')}
        workbook_permission_set = {k: v for (k, v) in permission_set.items() if k.startswith('Workbook')}
        datasource_permission_set = {k: v for (k, v) in permission_set.items() if k.startswith('Datasource')}

        if project_permission_set:
            self.add_permissions_for_group_project(project_id, group_id, project_permission_set)

        if workbook_permission_set:
            self.add_permissions_for_group_default_workbook(project_id, group_id, workbook_permission_set)

        if datasource_permission_set:
            self.add_permissions_for_group_default_datasource(project_id, group_id, datasource_permission_set)
