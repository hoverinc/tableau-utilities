import os
import shutil
from zipfile import ZipFile
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, TableDefinition, TableName, SqlType

from tableau_utilities.tableau_file.tableau_file import TableauFileError


def empty_extract(Datasource):
    """ Creates an empty extract (.hyper file) for the Tableau file.
        If the extract exists, it will be overwritten.

        Args:
            Datasource: The tableau_utilities datasource class

    """
    # Get relevant paths, and create a temp folder and move the Tableau file into it
    temp_folder = os.path.join(Datasource.file_directory, f'__TEMP_{Datasource.file_name}')
    extract_folder = os.path.join(temp_folder, 'Data', 'Extracts')
    hyper_rel_path = os.path.join('Data', 'Extracts', f'{Datasource.file_name}.hyper')
    temp_path = os.path.join(temp_folder, Datasource.file_basename)
    tdsx_basename = f'{Datasource.file_name}.tdsx'
    tdsx_path = os.path.join(temp_folder, tdsx_basename)
    os.makedirs(extract_folder, exist_ok=True)
    shutil.move(Datasource.file_path, temp_path)
    if Datasource.extension == 'tdsx':
        # Unzip the TDS file
        with ZipFile(temp_path) as z:
            for f in z.filelist:
                ext = f.filename.split('.')[-1]
                if ext in ['tds', 'twb']:
                    tds_path = z.extract(member=f, path=temp_folder)
    else:
        tds_path = temp_path
    hyper_path = os.path.join(extract_folder, f'{Datasource.file_name}.hyper')
    params = {"default_database_version": "2"}
    # Get columns from the metadata
    columns = dict()  # Use a dict to ensure no duplicate columns are referenced
    for metadata in Datasource.connection.metadata_records:
        if metadata.local_type == 'integer':
            column = TableDefinition.Column(metadata.remote_name, SqlType.int())
        elif metadata.local_type == 'real':
            column = TableDefinition.Column(metadata.remote_name, SqlType.double())
        elif metadata.local_type == 'string':
            column = TableDefinition.Column(metadata.remote_name, SqlType.varchar(metadata.width or 1020))
        elif metadata.local_type == 'boolean':
            column = TableDefinition.Column(metadata.remote_name, SqlType.bool())
        elif metadata.local_type == 'datetime':
            column = TableDefinition.Column(metadata.remote_name, SqlType.timestamp())
        elif metadata.local_type == 'date':
            column = TableDefinition.Column(metadata.remote_name, SqlType.date())
        else:
            raise TableauFileError(f'Got unexpected metadata type for hyper table: {metadata.local_type}')
        columns[metadata.remote_name] = column
    # Create an empty .hyper file based on the metadata of the Tableau file
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=params) as hyper:
        with Connection(hyper.endpoint, hyper_path, CreateMode.CREATE_AND_REPLACE) as connection:
            # Create an `Extract` table inside an `Extract` schema
            connection.catalog.create_schema('Extract')
            table = TableDefinition(TableName('Extract', 'Extract'), columns.values())
            connection.catalog.create_table(table)
    # Archive the extract with the TDS file
    with ZipFile(tdsx_path, 'w') as z:
        z.write(tds_path, arcname=os.path.basename(tds_path))
        z.write(hyper_path, arcname=hyper_rel_path)
    # Update datasource extract to reference .hyper file
    if Datasource.extract:
        Datasource.extract.connection.class_name = 'hyper'
        Datasource.extract.connection.authentication = 'auth-none'
        Datasource.extract.connection.author_locale = 'en_US'
        Datasource.extract.connection.extract_engine = None
        Datasource.extract.connection.dbname = hyper_rel_path
    # Move the tdsx out of the temp_folder and delete temp_folder
    Datasource.file_path = os.path.join(Datasource.file_directory, tdsx_basename)
    Datasource.file_basename = tdsx_basename
    Datasource.extension = 'tdsx'
    shutil.move(tdsx_path, Datasource.file_path)
    shutil.rmtree(temp_folder, ignore_errors=True)


def filter_extract(Datasource, delete_condition):
    """ Filters the data in the extract (.hyper file) for the Tableau file.

    Args:
        Datasource: The tableau_utilities datasource class
        delete_condition (str): A condition string to add to the WHERE clause of data to delete.
    """
    if Datasource.extension != 'tdsx' or not Datasource.has_extract_data:
        return None
    # Get relevant paths, and create a temp folder and move the Tableau file into it
    temp_folder = os.path.join(Datasource.file_directory, f'__TEMP_{Datasource.file_name}')
    temp_path = os.path.join(temp_folder, Datasource.file_basename)
    os.makedirs(temp_folder, exist_ok=True)
    shutil.move(Datasource.file_path, temp_path)
    # Unzip the TDS file
    unzipped_files = list()
    with ZipFile(temp_path) as z:
        for f in z.filelist:
            ext = f.filename.split('.')[-1]
            path = z.extract(member=f, path=temp_folder)
            unzipped_files.append(path)
            if ext == 'hyper':
                hyper_path = path
    # Update .hyper file based on the filter condition
    with HyperProcess(Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, hyper_path, CreateMode.NONE) as connection:
            connection.execute_command(f'DELETE FROM "Extract"."Extract" WHERE {delete_condition}')
    # Archive the extract with the TDS file
    with ZipFile(temp_path, 'w') as z:
        for file in unzipped_files:
            arcname = file.split(temp_folder)[-1]
            z.write(file, arcname=arcname)
    # Move the tdsx out of the temp_folder and delete temp_folder
    shutil.move(temp_path, Datasource.file_path)
    shutil.rmtree(temp_folder, ignore_errors=True)