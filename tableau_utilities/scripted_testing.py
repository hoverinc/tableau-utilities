import os
import yaml
import shutil
import xml.etree.ElementTree as ET
from pprint import pprint, isreadable
from tableau_utilities import Datasource, TableauServer
from tableau_utilities import tableau_file_objects as tfo


if __name__ == '__main__':
    with open('settings.yaml') as f:
        settings = yaml.safe_load(f)
    tmp_folder = 'tmp_downloads'
    # Cleanup lingering files, if there are any
    # shutil.rmtree(tmp_folder, ignore_errors=True)

    # Create a temp directory for testing
    os.makedirs(tmp_folder, exist_ok=True)
    os.chdir(tmp_folder)

    #  ### ### Testing here ### ###

    # datasource = Datasource('../resources/test_data_source.tdsx')
    # datasource.unzip()
    # datasource = Datasource('../resources/no_folder.tdsx')
    # datasource.unzip()
    datasource = Datasource('test_data_source.tds')
    # datasource = Datasource('no_folder (local copy).tds')
    # folder = tfo.Folder('Test')
    # datasource.folders_common.add(folder)
    # datasource.save()
    # datasource = Datasource('test_data_source.tds')
    # datasource = Datasource('no_folder (local copy).tds')
    pprint(datasource.folders_common)
    # datasource.save()
    # datasource.unzip()
    # Connect to Tableau Server
    # ts = TableauServer(**settings['tableau_login'])
    # ts.get_datasource(datasource_id='Jobs')
    # datasources = [(d.id, d.name) for d in ts.get_datasources()]
    # pprint(datasources)
    # Loop through the datasources
    # for d in datasources:
    # for path in os.listdir():
    #     if path != 'Jobs.tdsx':
    #         continue
        # path = ts.download_datasource(datasource_id=d.id, include_extract=True)
        # print('Downloaded:', path)
        # datasource = Datasource(path)
        # pprint(datasource.folders_common.folder)
        # print(datasource.columns['job_id'].caption)
        # datasource.columns['job_id'].caption = 'Job ID RENAMED'
        # datasource._tree.write(path.replace('.tdsx',  '.tds'))
        # datasource.save()
        # datasource = Datasource(path)
        # print(datasource.columns['job_id'].caption)
        # column = tfo.Column(
        #     name='FRIENDLY_NAME',
        #     caption='Friendly Name',
        #     datatype='string',
        #     type='ordinal',
        #     role='dimension',
        #     desc='Nice and friendly',
        # )
        # datasource.enforce_column(column, remote_name='ORG_ID', folder_name='Org')
        # print(datasource.folders.get('Org'))
        # print(datasource.datasource_metadata.get('ORG_ID'))
        # print(datasource.extract_metadata.get('ORG_ID'))
        # print(datasource.datasource_mapping_cols.get(column.name))
        # print(datasource.extract_mapping_cols.get(column.name))

    # ### ### ### ### ### ### ###

    # Cleanup lingering files, if there are any
    # os.chdir('..')
    # shutil.rmtree(tmp_folder)
