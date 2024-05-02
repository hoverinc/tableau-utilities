from requests import Session
from tableau_utilities.core.tableau_server.static import validate_response


class Base:
    """ Base functionality inherited by TableauServer class, and Core TableauServer classes """
    def __init__(self, parent):
        self.session: Session = parent.session
        self.user: str = parent.user
        self._pw: str = parent._pw
        self._personal_access_token_secret: str = parent._personal_access_token_secret
        self.personal_access_token_name: str = parent.personal_access_token_name
        self.host: str = parent.host
        self.site: str = parent.site
        self.api: float = parent.api
        self._auth_token = parent._auth_token
        self.url: str = parent.url
        self.get = parent.get if hasattr(parent, 'get') else None

    def _get(self, url, headers=None, **params):
        """ GET request for the Tableau REST API

        Args:
            url (str): URL endpoint for GET call
            headers (dict): GET call header

        Returns: The response content as a JSON dict
        """

        res = self.session.get(url, headers=headers, **params)
        return validate_response(res)

    def _post(self, url, json=None, headers=None, **params):
        """ POST request for the Tableau REST API

        Args:
            url (str): URL endpoint for POST call
            json (dict): The POST call JSON payload
            headers (dict): POST call header

        Returns: The response content as a JSON dict
        """
        res = self.session.post(url, json=json, headers=headers, **params)
        return validate_response(res)

    def _put(self, url, json=None, headers=None, **params):
        """ PUT request for the Tableau REST API

        Args:
            url (str): URL endpoint for PUT call
            json (dict): The PUT call JSON payload
            headers (dict): PUT call header

        Returns: The response content as a JSON dict
        """
        res = self.session.put(url, json=json, headers=headers, **params)
        return validate_response(res)

    def _delete(self, url, headers=None, **params):
        """ DELETE request for the Tableau REST API

        Args:
            url (str): URL endpoint for DELETE call
            headers (dict): DELETE call header

        Returns: The response content as a JSON dict
        """
        res = self.session.delete(url, headers=headers, **params)
        return validate_response(res)
