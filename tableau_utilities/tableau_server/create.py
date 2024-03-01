from requests import Session
from tableau_utilities.tableau_server.base import Base


class Create(Base):
    """ Core Create functionality of the TableauServer class """
    def __init__(self, parent):
        super().__init__(parent)

    def project(self, name, description='', content_permissions='LockedToProject'):
        """ Creates a project.

        Args:
            name (str): The name of the project
            description (str): The description of the project
            content_permissions (str): The content permissions, e.g. LockedToProject
        """
        self._post(
            f'{self.url}/projects',
            {
                'project': {
                    'name': name,
                    'description': description,
                    'contentPermissions': content_permissions
                }
            }
        )

    def group(self, name, minimum_site_role='Viewer'):
        """ Creates a group.

        Args:
            name (str): The name of the Group
            minimum_site_role (str): The minimum site role of the group, e.g. Viewer
        """
        self._post(
            f'{self.url}/groups',
            {
                'group': {
                    'name': name,
                    'minimumSiteRole': minimum_site_role
                }
            }
        )
