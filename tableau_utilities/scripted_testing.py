import os
import yaml
import shutil
import xml.etree.ElementTree as ET
from pprint import pprint, isreadable
from time import time
from tableau_utilities import Datasource, TableauServer
from tableau_utilities import tableau_file_objects as tfo


if __name__ == '__main__':
    start = time()
    with open('../settings.yaml') as f:
        creds = yaml.safe_load(f)['tableau_login']
        personal_access_token_name = creds['token_name']
        personal_access_token_secret = creds['token_secret']
        site = creds['site']
        host = f'https://{creds["server"]}.online.tableau.com'
        api_version = creds['api_version']
    tmp_folder = 'tmp_downloads'
    # Cleanup lingering files, if there are any
    # shutil.rmtree(tmp_folder, ignore_errors=True)

    # Create a temp directory for testing
    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)

    #  ### ### Testing here ### ###
    # RAN IN --> 217 seconds
    # tableau_auth = TSC.PersonalAccessTokenAuth(
    #     personal_access_token_name,
    #     personal_access_token_secret,
    #     site_id=site
    # )
    # server = TSC.Server(host, use_server_version=True)
    # server.auth.sign_in(tableau_auth)
    # datasource_item = [d for d in server.datasources.get()[0] if d.id == dsid][0]
    # server.datasources.publish(datasource_item, path, 'Overwrite')
    # print('Published')

    # datasource = Datasource('../resources/test_data_source.tdsx')
    # datasource.unzip()
    # datasource = Datasource('../resources/no_folder.tdsx')
    # datasource.unzip()
    # datasource = Datasource('test_data_source.tds')
    # datasource = Datasource('no_folder (local copy).tds')
    # folder = tfo.Folder('Test')
    # datasource.folders_common.add(folder)
    # datasource.save()
    # datasource = Datasource('test_data_source.tds')
    # datasource = Datasource('no_folder (local copy).tds')
    # pprint(datasource.folders_common)
    # datasource.save()
    # datasource.unzip()

    # Loop through the datasources
    # for d in datasources:
    # for path in os.listdir():
    #     if path != 'Jobs.tdsx':
    #         continue
    #     path = ts.download_datasource(datasource_id=d.id, include_extract=True)
    #     print('Downloaded:', path)
    #     datasource = Datasource(path)
    #     pprint(datasource.folders_common.folder)
    #     print(datasource.columns['job_id'].caption)
    #     datasource.columns['job_id'].caption = 'Job ID RENAMED'
    #     datasource._tree.write(path.replace('.tdsx',  '.tds'))
    #     datasource.save()
    #     datasource = Datasource(path)
    #     print(datasource.columns['job_id'].caption)
    #     column = tfo.Column(
    #         name='FRIENDLY_NAME',
    #         caption='Friendly Name',
    #         datatype='string',
    #         type='ordinal',
    #         role='dimension',
    #         desc='Nice and friendly',
    #     )
    #     datasource.enforce_column(column, remote_name='ORG_ID', folder_name='Org')
    #     print(datasource.folders.get('Org'))
    #     print(datasource.datasource_metadata.get('ORG_ID'))
    #     print(datasource.extract_metadata.get('ORG_ID'))
    #     print(datasource.datasource_mapping_cols.get(column.name))
    #     print(datasource.extract_mapping_cols.get(column.name))

    # ### ### ### ### ### ### ###

    # Cleanup lingering files, if there are any
    # os.chdir('..')
    # shutil.rmtree(tmp_folder)
    print(f'Script finished in {round(time() - start)} seconds')
