import pytest
import tableau_utilities as tu
import tableau_utilities.tableau_file.tableau_file_objects as tfo
import shutil
import os


EXTRACT_PATH = 'test_data_source.tdsx'
LIVE_PATH = 'test_live_data_source.tdsx'
ONE_FOLDER_PATH = 'one_folder.tdsx'
NO_FOLDER_PATH = 'no_folder.tdsx'

COLUMN = tfo.Column(
    name='FRIENDLY_NAME',
    caption='Friendly Name',
    datatype='string',
    type='ordinal',
    role='dimension',
    desc='Nice and friendly',
)
FOLDER = tfo.Folder(name='Friendly Name')


# cli.parser args
def test_cli_local_args():
    argv = [
        '-l', 'local',
        '-f', EXTRACT_PATH,
        '-tds',
        'datasource'
    ]
    args = tu.cli.parser.parse_args(argv)
    assert args.file_path == EXTRACT_PATH


# cli.parser args
def test_cli_server_info_args():
    argv = [
        '-tn', 'token_name',
        '-ts', 'token_secret',
        '-sn', 'nice_site',
        '-s', 'us-east-1',
        'server_info',
        '--list_object', 'datasource',
        '--list_format', 'ids_names',
        '--list_sort_field', 'id',
    ]
    args = tu.cli.parser.parse_args(argv)
    assert args.server == 'us-east-1'


# Column()
def test_column_attributes():
    column_attributes = {
        'name': '[FRIENDLY_CALC]',
        'caption': 'Friendly Calc',
        'datatype': 'integer',
        'type': 'ordinal',
        'role': 'dimension',
        'desc': 'Nice and friendly',
    }
    column1 = tfo.Column(calculation='count(1)', **column_attributes)
    column2 = tfo.Column(calculation={'@formula': 'count(1)', '@class': 'tableau'}, **column_attributes)
    for attr, value in column_attributes.items():
        assert value == getattr(column1, attr)
        if attr == 'calculation':
            assert 'count(1)' == getattr(column2, attr)
        else:
            assert value == getattr(column2, attr)


# Datasource().DatasourceItems
def test_datasource_items():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    assert datasource.columns is not None and datasource.columns != []
    assert datasource.folders_common is not None and datasource.folders_common != []
    assert datasource.aliases is not None and datasource.aliases != []
    assert datasource.connection is not None and datasource.connection != []
    assert datasource.extract is not None and datasource.extract != []


# Datasource().unzip()
def test_unzip_tableau_file():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    path = tu.Datasource(EXTRACT_PATH).unzip()
    with open(path) as f:
        content = f.read()
    os.remove(EXTRACT_PATH)
    os.remove(path)
    assert content is not None


# Datasource().save()
def test_datasource_save():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource_before = tu.Datasource(EXTRACT_PATH)
    datasource_before.save()
    datasource_after = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    for before, after in zip(datasource_before.sections(), datasource_after.sections()):
        assert before == after


# Datasource().columns.add()
def test_add_column():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    datasource.columns.add(COLUMN)
    column = datasource.columns.get(COLUMN)
    assert column is not None


# Datasource().enforce_column()
def test_enforce_column():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    col = tfo.Column(
        name='renamed_name',
        datatype='string',
        role='dimension',
        type='nominal',
        caption='Renamed Name'
    )
    datasource.enforce_column(col, folder_name='A New Folder', remote_name='NAME')
    column = datasource.columns.get(col)
    folder = datasource.folders_common.get('A New Folder')
    metadata = datasource.connection.metadata_records.get('NAME')
    assert column == col
    assert folder is not None
    assert metadata.local_name == '[renamed_name]'


# Datasource().columns.add()
def test_add_existing_column():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    datasource.columns.add(COLUMN)
    datasource.columns.add(COLUMN)
    assert len([c for c in datasource.columns if c == COLUMN]) == 1


# Datasource().folders_common.folder.add()
def test_add_folder_tdsx_has_folder():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    datasource.folders_common.add(FOLDER)
    found_folder = datasource.folders_common.get(FOLDER)
    assert found_folder is not None


# Datasource().folders_common.folder.add()
def test_add_folder_tdsx_does_not_have_folder():
    shutil.copyfile(f'resources/{NO_FOLDER_PATH}', NO_FOLDER_PATH)
    datasource = tu.Datasource(NO_FOLDER_PATH)
    os.remove(NO_FOLDER_PATH)
    datasource.folders_common.add(FOLDER)
    found_folder = datasource.folders_common.get(FOLDER)
    assert found_folder is not None


# Datasource().folders_common.folder.add()
def test_add_folder_tdsx_has_one_folder():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    datasource.folders_common.add(FOLDER)
    found_folder = datasource.folders_common.get(FOLDER)
    assert found_folder is not None


# Datasource().folders_common.add()
def test_add_existing_folder():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    datasource.folders_common.add(FOLDER)
    datasource.folders_common.add(FOLDER)
    assert len([f for f in datasource.folders_common if f == FOLDER]) == 1


# Datasource().folders_common.delete()
def test_delete_folder():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    datasource.folders_common.add(FOLDER)
    datasource.folders_common.delete(FOLDER)
    found_folder = datasource.folders_common.get(FOLDER)
    assert not found_folder


# Datasource().columns.update()
def test_update_column():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    datasource = tu.Datasource(EXTRACT_PATH)
    os.remove(EXTRACT_PATH)
    column = tfo.Column(
        name='QUANTITY',
        caption='Quantity Renamed',
        datatype="integer",
        role="measure",
        type="quantitative"
    )
    datasource.columns.update(column)
    col = datasource.columns.get(column)
    assert col.caption == 'Quantity Renamed'


# Datasource().folders_common.get()
def test_get_folder():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    shutil.copyfile(f'resources/{LIVE_PATH}', LIVE_PATH)
    shutil.copyfile(f'resources/{ONE_FOLDER_PATH}', ONE_FOLDER_PATH)
    ds_extract = tu.Datasource(EXTRACT_PATH)
    ds_live = tu.Datasource(LIVE_PATH)
    ds_one_folder = tu.Datasource(ONE_FOLDER_PATH)
    os.remove(EXTRACT_PATH)
    os.remove(LIVE_PATH)
    os.remove(ONE_FOLDER_PATH)
    assert ds_extract.folders_common.get('tidy') is not None
    assert ds_live.folders_common.get('tidy') is None
    assert ds_one_folder.folders_common.get('neat') is not None


# Datasource().folders_common
def test_find_all_folders():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    shutil.copyfile(f'resources/{LIVE_PATH}', LIVE_PATH)
    shutil.copyfile(f'resources/{ONE_FOLDER_PATH}', ONE_FOLDER_PATH)
    ds_extract = tu.Datasource(EXTRACT_PATH)
    ds_live = tu.Datasource(LIVE_PATH)
    ds_one_folder = tu.Datasource(ONE_FOLDER_PATH)
    os.remove(EXTRACT_PATH)
    os.remove(LIVE_PATH)
    os.remove(ONE_FOLDER_PATH)
    # The new xml structure does not specify role
    _folder = ds_extract.folders_common
    _folder_live = ds_live.folders_common
    _folder_one_folder = ds_one_folder.folders_common
    assert _folder == ['neat', 'tidy']
    assert _folder_live == []
    assert _folder_one_folder == ['neat']


# Datasource().columns
def test_find_all_columns():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    shutil.copyfile(f'resources/{LIVE_PATH}', LIVE_PATH)
    ds_extract = tu.Datasource(EXTRACT_PATH)
    ds_live = tu.Datasource(LIVE_PATH)
    os.remove(EXTRACT_PATH)
    os.remove(LIVE_PATH)
    assert ds_extract.columns == [
        'CREATED_AT',
        'ID',
        'NAME',
        'Number of Records',
        'QUANTITY',
        '[__tableau_internal_object_id__].[Migrated Data]'
    ]
    assert ds_live.columns == [
        'Number of Records',
        '[__tableau_internal_object_id__].[Migrated Data]'
    ]


# Datasource().connection.relation.connection
def test_find_all_connections():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    shutil.copyfile(f'resources/{LIVE_PATH}', LIVE_PATH)
    ds_extract = tu.Datasource(EXTRACT_PATH)
    ds_live = tu.Datasource(LIVE_PATH)
    os.remove(EXTRACT_PATH)
    os.remove(LIVE_PATH)
    assert ds_extract.connection.named_connections == ['snowflake']
    assert ds_live.connection.named_connections == ['snowflake']


# get_connections()
def test_get_connection():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    shutil.copyfile(f'resources/{LIVE_PATH}', LIVE_PATH)
    ds_extract = tu.Datasource(EXTRACT_PATH)
    ds_live = tu.Datasource(LIVE_PATH)
    os.remove(EXTRACT_PATH)
    os.remove(LIVE_PATH)
    assert ds_extract.connection.named_connections.get('snowflake') is not None
    assert ds_live.connection.named_connections.get('snowflake') is not None


# Datasource().connection.update()
def test_update_connection():
    shutil.copyfile(f'resources/{EXTRACT_PATH}', EXTRACT_PATH)
    shutil.copyfile(f'resources/{LIVE_PATH}', LIVE_PATH)
    ds_extract = tu.Datasource(EXTRACT_PATH)
    ds_live = tu.Datasource(LIVE_PATH)
    os.remove(EXTRACT_PATH)
    os.remove(LIVE_PATH)
    connection = tfo.Connection(
        class_name='snowflake',
        dbname='FAKE_DB',
        schema='FAKE_SCHEMA',
        server='my_account_name.snowflakecomputing.com',
        service='FAKE_ROLE',
        username='FAKE_USER',
        warehouse='FAKE_WAREHOUSE'
    )
    ds_extract.connection.update(connection)
    ds_live.connection.update(connection)
    assert ds_extract.connection.named_connections['snowflake'].caption == connection.server
    assert ds_live.connection.named_connections['snowflake'].caption == connection.server
    assert ds_extract.connection['snowflake'].dbname == 'FAKE_DB'
    assert ds_live.connection['snowflake'].dbname == 'FAKE_DB'
