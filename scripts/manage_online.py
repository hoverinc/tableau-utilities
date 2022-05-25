# pylint: disable=C0301
# keep long urls on one line for readabilty
"""
# This script prints out users by Tableau Server group by site
#
# To run the script, you must have installed Python 2.7.9 or later,
# plus the 'requests' library:
#   http://docs.python-requests.org/en/latest/
#
# Run the script in a terminal window by entering:
#   python users_by_group.py <server_address> <username>
#
#   You will be prompted for site id, and group name
#   There is also an option to print out all groups
#   See the main() method for details
#
# This script requires a server administrator or a site administrator.
#
# The file version.py must be in the local folder with the correct API version number
"""

import xml.etree.ElementTree as ET # Contains methods used to build and parse XML
import sys
import getpass
import requests  # Contains methods used to make HTTP requests
import permissions_sets as PS
import permission_project_groups as PPG
from permissions_groups import GroupProjectPermissions
from collections import defaultdict
from settings import tableau_credentials, VERSION

# from urllib2 import Request, urlopen

# The namespace for the REST API is 'http://tableausoftware.com/api' for Tableau Server 9.0
# or 'http://tableau.com/api' for Tableau Server 9.1 or later
XMLNS = {'t': 'http://tableau.com/api'}


class ApiCallError(Exception):
    """ ApiCallError """
    pass


class UserDefinedFieldError(Exception):
    """ UserDefinedFieldError """
    pass


def _encode_for_display(text):
    """ Encodes strings so they can display as ASCII in a Windows terminal window.
        This function also encodes strings for processing by xml.etree.ElementTree functions.
        Unicode characters are converted to ASCII placeholders (for example, "?").

    Args:
        text (str): The text to encode

    Returns: An ASCII-encoded version of the text
    """
    return text.encode('ascii', errors="backslashreplace").decode('utf-8')


def _check_status(server_response, success_code):
    """ Checks the server response for possible errors.

    Args:
        server_response (obj): The response received from the server
        success_code (int): The expected success code for the response
    Throws an ApiCallError exception if the API call fails.
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
        error_message = f'{code}: {summary} - {detail}'
        raise ApiCallError(error_message)
    return


def sign_in(server, username, password, site=""):
    """ Signs in to the server specified with the given credentials
        Note that most of the functions in this example require that the user
        have server administrator permissions.

    Args:
        server (str): Specified server address
        username (str): The name (not ID) of the user to sign in as.
        password (str): The password for the user.
        site (str): The ID of the site on the server to sign in to.
            The default ("") signs in to the default site.

    Returns: The authentication token and the site_id
    """
    url = f"{server}/api/{VERSION}/auth/signin"

    # Builds the request
    xml_request = ET.Element('tsRequest')
    credentials_element = ET.SubElement(xml_request, 'credentials', name=username, password=password)
    ET.SubElement(credentials_element, 'site', contentUrl=site)
    xml_request = ET.tostring(xml_request)

    # Make the request to server
    server_response = requests.post(url, data=xml_request)
    _check_status(server_response, 200)

    # ASCII encode server response to enable displaying to console
    server_response = _encode_for_display(server_response.text)

    # Reads and parses the response
    parsed_response = ET.fromstring(server_response)

    # Gets the auth token and site ID
    token = parsed_response.find('t:credentials', namespaces=XMLNS).get('token')
    site_id = parsed_response.find('.//t:site', namespaces=XMLNS).get('id')
    # user_id = parsed_response.find('.//t:user', namespaces=XMLNS).get('id')
    return token, site_id


def sign_out(server, auth_token):
    """ Destroys the active session and invalidates authentication token.

    Args:
        server (str): Specified server address
        auth_token (str): Authentication token that grants user access to API calls
    """
    url = f"{server}/api/{VERSION}/auth/signout"
    server_response = requests.post(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 204)
    return


def get_group_id(server, auth_token, site_id, group_name):
    """
    Args:
        server (str): Specified server address
        auth_token (str): Authentication token that grants user access to API calls
        site_id (str): The Tableau site ID
        group_name (str): The Tableau group name

    Returns: The group ID for the group name
    """
    url = f"{server}/api/{VERSION}/sites/{site_id}/groups"
    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))

    groups = xml_response.findall('.//t:group', namespaces=XMLNS)
    for group in groups:
        if group.get('name') == group_name:
            return group.get('id')
    error = f"Group named '{group_name}' not found."
    raise LookupError(error)


def query_projects(server, auth_token, site_id, page_size, page_number):
    """ Queries and returns all projects in the site

        URI GET /api/api-version/sites/site-id/projects
        GET /api/api-version/sites/site-id/projects?pageSize=page-size&pageNumber=page-number

    Args:
        server (str): Specified server address
        auth_token (str): Authentication token that grants user access to API calls
        site_id (str): The Tableau site ID
        page_size (int): The page size
        page_number (int): The page number

    Returns (list): The list of projects
    """
    url = f"{server}/api/{VERSION}/sites/{site_id}/projects"
    if page_size and page_size > 0:
        url += f"?pageSize={page_size}&pageNumber={page_number}"

    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    projects = xml_response.findall('.//t:project', namespaces=XMLNS)
    return projects


def query_groups(server, auth_token, site_id, page_size, page_number):
    """ Queries and returns all groups in the site

        URI GET /api/api-version/sites/site-id/groups
        GET /api/api-version/sites/site-id/groups?pageSize=page-size&pageNumber=page-number

    Args:
        server (str): Specified server address
        auth_token (str): Authentication token that grants user access to API calls
        site_id (str): The Tableau site ID
        page_size (int): The page size
        page_number (int): The page number

    Returns (list): The list of groups
    """
    url = f"{server}/api/{VERSION}/sites/{site_id}/groups"
    if page_size and page_size > 0:
        url += f"?pageSize={page_size}&pageNumber={page_number}"

    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    groups = xml_response.findall('.//t:group', namespaces=XMLNS)
    return groups


def get_users_in_group(server, auth_token, site_id, group_id, page_size, page_number):
    """ Queries and returns all users in the group using group id

        GET /api/api-version/sites/site-id/groups/group-id/users
        GET /api/api-version/sites/site-id/groups/group-id/users?pageSize=page-size&pageNumber=page-number

    Args:
        server (str): Specified server address
        auth_token (str): Authentication token that grants user access to API calls
        site_id (str): The Tableau site ID
        group_id (str): The Tableau group ID
        page_size (int): The page size
        page_number (int): The page number

    Returns (list): The list of users
    """
    url = f"{server}/api/{VERSION}/sites/{site_id}/groups/{group_id}/users"
    if page_size and page_size > 0:
        url += f"?pageSize={page_size}&pageNumber={page_number}"

    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    # _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    users = xml_response.findall('.//t:user', namespaces=XMLNS)
    return users


def get_users_in_group_count(server, auth_token, site_id, group_id):
    """ Find out how many users are available in the group

        GET /api/api-version/sites/site-id/groups/group-id/users

    Args:
        server (str): Specified server address
        auth_token (str): Authentication token that grants user access to API calls
        site_id (str): The Tableau site ID
        group_id (str): The Tableau group ID

    Returns (int): The total users that are available in the group
    """
    url = f"{server}/api/{VERSION}/sites/{site_id}/groups/{group_id}/users"
    server_response = requests.get(url, headers={'x-tableau-auth': auth_token})
    # _check_status(server_response, 200)
    xml_response = ET.fromstring(_encode_for_display(server_response.text))
    total_available = xml_response.find('.//t:pagination', namespaces=XMLNS).attrib['totalAvailable']
    return int(total_available)


def print_user_group_audit(server, auth_token, site_id, page_size, total_returned, groups, group_name,
                           test_user_list, exclude_test_users=True, print_users=True):
    """ Print audit of the domains and users in each group.
        Exclude test users by default.

    Args:
        server (str): Specified server address
        auth_token (str): Authentication token that grants user access to API calls
        site_id (str): The Tableau site ID
        page_size (int): The page size
        total_returned (int): The total results returned so far
        groups (list): A list of group objects
        group_name (str): The Tableau group name
        test_user_list (list): A list of test user names
        exclude_test_users (bool): If test users should not be listed
        print_users (bool): If users should be printed to the terminal
    """
    for group in groups:
        done = False

        # This method counts from 1
        counter = 1

        group_id = group.get('id')
        total_available = get_users_in_group_count(server, auth_token, site_id, group_id)

        usa_groups_domains = []

        if group_name != "" and group.get('name') != group_name:
            continue

        print("\nGROUP ", group.get('name'), "USERS: ", str(total_available))
        # print("\nPrinting " + str(total_available) + ' users from the group: ' + group.get('name'))
        while not done:

            domains = []

            users = get_users_in_group(server, auth_token, site_id, group_id, page_size, counter)
            counter += 1
            for user in users:
                username = user.get('name')

                if print_users:
                    print(username)

                if exclude_test_users and username in test_user_list:
                    domain = False
                else:
                    domain = username.split('@')[1]

                if domain not in domains:
                    domains.append(domain)

                if group_name.find('USA') and domain not in usa_groups_domains:
                    usa_groups_domains.append(domain)

            print('GROUP: ', group.get('name'), 'DOMAINS: ', domains)

            total_returned = total_returned + page_size
            if total_returned >= total_available:
                done = True


def get_permission_map_name(project_name, group_name):
    """ Gets the permissions for a specific group and project,
        as it has been configured in permission_project_groups -> permission_mappings

    Args:
        project_name (str): The project name
        group_name (str): The group name

    Returns: The permissions
    """
    for map in PPG.permission_mappings:
        if project_name == map['project_name'] and group_name == map['group_name']:
            return map['permission_set_name']

    return 'deny_all'


def get_workbook_view_csv(auth_token, view_csv_url):
    """ Gets the contents of a CSV from a Tableau workbook's view,
        from provided URL.

    Args:
        auth_token (str): Authentication token that grants user access to API calls
        view_csv_url: The URL to the CSV of a workbook's view
            i.e. 'https://<server>.online.tableau.com/#/site/<site>/views/<WorkbookName>/<ViewName>.csv'

    Returns: The contents of the CSV
    """
    from urllib.request import Request, urlopen

    request = Request(view_csv_url)
    request.add_header('Cookie', 'workgroup_session_id='+auth_token)
    request.add_header('Connection', 'keep-alive')
    return urlopen(request).read()


def main():
    """
    To automate this script then fill in the values for server, username, etc
    You will be prompted for any values set to ""

    Server and username can be entered on the command line as well.

      users_by_group.py http://localhost username

    """
    # Parameters
    server = tableau_credentials["server"]
    username = tableau_credentials["username"]
    password = tableau_credentials["password"]
    site_id = tableau_credentials["site_id"]
    group_name = "All"
    page_size = 100
    test_user_list = []

    print("\nSigning in to obtain authentication token")
    auth_token, site_id = sign_in(server, username, password, site_id)

    total_available = 0
    total_returned = 0

    # get all the groups in the site and create a dictionary with the name and id
    groups = query_groups(server, auth_token, site_id, 0, 0)
    group_name = ""
    group_dict = {}
    for group in groups:
        group_dict[group.get('name')] = group.get('id')

    # get all the projects in the site
    projects = query_projects(server, auth_token, site_id, 0, 0)
    all_project_dict = {}
    top_level_project_dict = {}
    for project in projects:
        all_project_dict[project.get('name')] = project.get('id')
        if project.get('parentProjectId') is None:
            top_level_project_dict[project.get('name')] = project.get('id')

    # audits the users in each group
    print_user_group_audit(server, auth_token, site_id, page_size, total_returned, groups, group_name,
                           test_user_list, exclude_test_users=False, print_users=False)

    # Start Permissions Groups Class
    GPP = GroupProjectPermissions(server, auth_token, site_id)

    # Lock or verify lock to project on all projects
    GPP.lock_all_permissions_to_the_project(projects)

    for project_name, project_id in top_level_project_dict.items():
        for group_name, group_id in group_dict.items():
            # These groups have no permissions and permissions for these users are set via other groups
            if group_name not in ['All Users', 'Data Team']:
                print("PROJECT: ", project_name, "GROUP: ", group_name)

                # Get the existing permissions for a group and project
                existing_permission_set = GPP.get_existing_permissions_for_project_group(project_id, group_id)

                # Get the expected permission set for a group and project
                desired_permission_set_name = get_permission_map_name(project_name, group_name)
                # print(desired_permission_set_name)
                desired_permission_set = eval('PS.' + desired_permission_set_name)

                # print(desired_permission_set)
                # print(existing_permission_set)

                # Compare the overall set
                if existing_permission_set == desired_permission_set:
                    print("Permissions are good")
                else:
                    print("Adjusting permissions")
                    print(desired_permission_set_name)
                    print(desired_permission_set)

                    # permissions to delete - Existing permissions that are not desired
                    permissions_to_delete = GPP.dict_not_intersect(existing_permission_set, desired_permission_set)
                    print(permissions_to_delete)

                    # permissions to apply - Desired permissions that are not in the existing permission
                    permissions_to_apply = GPP.dict_not_intersect(desired_permission_set, existing_permission_set)
                    print(permissions_to_apply)

                    # delete permissions
                    GPP.delete_permission_set(project_id, group_id, permissions_to_delete)

                    # add permissions
                    GPP.add_permission_set(project_id, group_id, permissions_to_apply)

    # print("---")
    # print('EXISTING PERMISSIONS:'
    # for k, v in existing_permission_set.iteritems():
    #     print(k, v)
    #
    # print("")
    # print("")
    # print("---")
    # print('DESIRED PERMISSIONS')
    # for k, v in desired_permission_set.iteritems():
    #     print(k, v)
    #
    # print("")
    # print("")
    # print("---")
    # print('PERMISSIONS TO DELETE')
    # for k, v in permissions_to_delete.iteritems():
    #     print(k, v)
    #
    # print("")
    # print("")
    # print("---")
    # print('PERMISSIONS TO APPLY')
    # for k, v in permissions_to_apply.iteritems():
    #     print(k, v)

    # # print(get_workbook_view_csv(auth_token, ''))
    # get_workbook_view_csv(auth_token, '')

    print("\nSigning out and invalidating the authentication token")
    sign_out(server, auth_token)


if __name__ == "__main__":
    main()
