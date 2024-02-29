import tableau_utilities.tableau_server.tableau_server_objects as tso
from requests import Session
from tableau_utilities.tableau_server.static import transform_tableau_object
from tableau_utilities.tableau_server.base import Base


class Update(Base):
    def __init__(self, parent):
        super().__init__(parent)

    def datasource_connection(self, datasource_id, connection: tso.Connection):
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

        content = self._put(
            f'{self.url}/datasources/{datasource_id}/connections/{connection.id}',
            json={'connection': conn_dict}
        )

        transform_tableau_object(content['connection'])
        return tso.Connection(**content['connection'])
