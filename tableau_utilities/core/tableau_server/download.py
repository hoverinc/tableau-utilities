import cgi
import os
import requests
from tableau_utilities.core.tableau_server.static import TableauConnectionError
from tableau_utilities.tableau_server.base import Base


class Download(Base):
    """ Core Download functionality of the TableauServer class """
    def __init__(self, parent):
        super().__init__(parent)

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

    def datasource(self, datasource_id, file_dir=None, include_extract=False):
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

    def workbook(self, workbook_id, file_dir=None, include_extract=False):
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
