import os
import logging
from time import time
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata
import tableau_utilities.tableau_server.tableau_server_objects as tso
from requests import Session
from tableau_utilities.tableau_server.static import (
    TableauConnectionError, bytes_to_mb, mb_to_bytes, transform_tableau_object)
from tableau_utilities.tableau_server.base import Base


class Publish(Base):
    """ Core Publish functionality of the TableauServer class """
    def __init__(self, parent):
        super().__init__(parent)

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
            r.make_multipart(content_type=content_type)
            part_list.append(r)
        post_body, content_type = encode_multipart_formdata(part_list)
        content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
        return post_body, content_type

    def __get_datasource_for_publication(self, datasource_id, datasource_name, project_name):
        if datasource_id:
            return self.get.datasource(datasource_id)
        elif datasource_name and project_name:
            project = None
            for p in self.get.projects():
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
            return self.get.workbook(workbook_id)
        elif workbook_name and project_name:
            project = None
            for p in self.get.projects():
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

    # 323 seconds at 5 mb, 145 seconds at 50mb
    def __upload_in_chunks(self, file_path, chunk_size_mb=5, log_interval=5):
        """ Uplaods a file to Tableau, in chunks.
            - PUT /api/api-version/sites/site-id/fileUploads
            - PUT /api/api-version/sites/site-id/fileUploads/upload_session_id

        Args:
            file_path (str): The path to the file
            chunk_size_mb (int): The chunking size of increments to be uploaded
            log_interval (int): The interval of megabytes uploaded to log progress of the upload.

        Returns: An upload_session_id of the uploaded file
        """
        start = time()
        file_name = os.path.basename(file_path)
        # Initialize file upload session
        res = self._post(f'{self.url}/fileUploads')
        upload_session_id = res['fileUpload']["uploadSessionId"]
        # Read file and append data in chunks
        total = round(bytes_to_mb(os.path.getsize(file_path)), 1)
        current = 0
        file = open(file_path, 'rb')
        while True:
            chunk = file.read(mb_to_bytes(chunk_size_mb))
            if not chunk:
                break
            post_body, content_type = self.__get_multipart_details([
                ('request_payload', '', None, 'text/xml'),
                ('tableau_file', chunk, file_name, 'application/octet-stream')
            ])
            # Log progress every so often
            current += chunk_size_mb
            if current == chunk_size_mb or current % 500 == 0 or current >= total:
                logging.info('({} of {} mb) Uploading {}'.format(
                    current if current < total else total, total, file_path))
            self._put(
                f'{self.url}/fileUploads/{upload_session_id}',
                data=post_body, headers={'Content-Type': content_type}
            )
        file.close()
        logging.info('Uploaded {}: {} mb in {} seconds'.format(file_path, total, round(time() - start)))
        return upload_session_id

    def datasource(self, file_path, datasource_id=None, datasource_name=None, project_name=None, **kw):
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
            upload_chunk_size (int): The number of megabytes that will be uploaded at a time. Max is 64, default is 64.
            upload_log_interval (int): The interval of megabytes when to log the progress of the upload. Default is 64.

        Returns: A Datasource Tableau server object
        """
        overwrite = kw.pop('overwrite', True)
        as_job = kw.pop('as_job', False)
        append = kw.pop('append', False)
        connection = kw.pop('connection', None)
        upload_chunk_size = kw.pop('upload_chunk_size', 64)
        upload_log_interval = kw.pop('upload_log_interval', 64)
        file_name = os.path.basename(file_path)
        extension = file_path.split('.')[-1]
        datasource = self.__get_datasource_for_publication(datasource_id, datasource_name, project_name)
        ds_xml = datasource.publish_xml(connection)
        # Datasource must be less than 64mb to publish all at once
        if bytes_to_mb(os.path.getsize(file_path)) >= 64:
            upload_session_id = self.__upload_in_chunks(file_path, upload_chunk_size, upload_log_interval)
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
        start = time()
        logging.info('Publishing uploaded datasource {}'.format(file_path))
        content = self._post(publish_url, data=post_body, headers={'Content-Type': content_type})
        logging.info('Published uploaded datasource {} in {} seconds'.format(file_path, round(time() - start)))
        transform_tableau_object(content['datasource'])
        return tso.Datasource(**content['datasource'])

    def workbook(self, file_path, workbook_id=None, workbook_name=None, project_name=None, **kw):
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
            upload_chunk_size (int): The number of megabytes that will be uploaded at a time. Max is 64, default is 64.
            upload_log_interval (int): The interval of megabytes when to log the progress of the upload. Default is 64.

        Returns: A Workbook Tableau server object
        """
        overwrite = kw.pop('overwrite', True)
        as_job = kw.pop('as_job', False)
        skip_connection_check = kw.pop('skip_connection_check', False)
        connections = kw.pop('connections', None)
        upload_chunk_size = kw.pop('upload_chunk_size', 64)
        upload_log_interval = kw.pop('upload_log_interval', 64)
        file_name = os.path.basename(file_path)
        extension = file_path.split('.')[-1]
        workbook = self.__get_workbook_for_publication(workbook_id, workbook_name, project_name)
        wb_xml = workbook.publish_xml(connections)
        # Datasource must be 64mb or less to publish all at once
        if bytes_to_mb(os.path.getsize(file_path)) > 64:
            upload_session_id = self.__upload_in_chunks(file_path, upload_chunk_size, upload_log_interval)
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
        start = time()
        logging.info('Publishing uploaded workbook {}'.format(file_path))
        content = self._post(publish_url, data=post_body, headers={'Content-Type': content_type})
        logging.info('Published uploaded workbook {} in {} seconds'.format(file_path, round(time() - start)))
        transform_tableau_object(content['workbook'])
        return tso.Workbook(**content['workbook'])
