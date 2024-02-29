import tableau_utilities.tableau_server.tableau_server_objects as tso
from requests import Session
from tableau_utilities.tableau_server.static import transform_tableau_object
from tableau_utilities.tableau_server.base import Base


class Refresh(Base):
    def __init__(self, parent):
        super().__init__(parent)

    def datasource(self, datasource_id):
        """ Refresh a datasource """
        content = self._post(f'{self.url}/datasources/{datasource_id}/refresh', json={})
        transform_tableau_object(content['job'])
        return tso.Job(**content['job'])

    def workbook(self, workbook_id):
        """ Refresh a workbook """
        content = self._post(f'{self.url}/workbooks/{workbook_id}/refresh', json={})
        transform_tableau_object(content['job'])
        return tso.Job(**content['job'])
