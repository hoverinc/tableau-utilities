import os
import shutil
import tableau_utilities.tableau_file.tableau_file_objects as tfo

from time import time
from tableau_utilities.general.config_column_persona import personas, get_persona_by_attribs, \
    get_persona_by_metadata_local_type
from tableau_utilities.general.cli_styling import Color
from tableau_utilities.general.cli_styling import Symbol
from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


# Define color and symbol as globals
color = Color()
symbol = Symbol()

def create_column(name: str, persona: dict):
    """ Creates the tfo column object with the minimum required fields to add a column

    Args:
        name: The name for the Column, and local_name of the Metadata Record.
        persona: A dictionary showing the role, datatype, and role type

    Returns:
        A tfo column object

    """

    column = tfo.Column(
        name=name,
        role=persona['role'],
        datatype=persona['datatype'],
        type=persona['role_type'],
    )

    return column


def add_metadata_records_as_columns(ds, debugging_logs=False):
    """ Adds <column /> records when they are only present in the <metadata-record class='column'>

    When you create your Tableau extract the first time all columns will be present in Metadata records like this:

      <metadata-record class='column'>
        <remote-name>MY_COLUMN</remote-name>
        <remote-type>131</remote-type>
        <local-name>[MY_COLUMN]</local-name>
        <parent-name>[Custom SQL Query]</parent-name>
        <remote-alias>MY_COLUMN</remote-alias>
        <ordinal>5</ordinal>
        <local-type>integer</local-type>
        <aggregation>Sum</aggregation>
        <precision>38</precision>
        <scale>0</scale>
        <contains-null>true</contains-null>
        <attributes>
          <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_DECIMAL&quot;</attribute>
          <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_NUMERIC&quot;</attribute>
        </attributes>
        <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_62A667B34C534415B10B2075B0DC36DC]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
      </metadata-record>

    Separately some columns may have a column like this:
    <column datatype='integer' name='[MY_COLUMN]' role='dimension' type='ordinal' />


    Manipulating Tableau columns requires a <column /> record.

    Args:
        ds: A Datasource object
        color: The cli color styling class
        debugging_logs: True to print debugging information to the console

    Returns:
        ds: An altered datasource. You'll still need to save this ds to apply the changes.

    """

    # Create the list of columns to add
    columns_to_add = [
        m for m in ds.connection.metadata_records
        if m.local_name not in [c.name for c in ds.columns]
    ]
    print(f'{color.fg_yellow}Adding missing columns from Metadata Records:{color.reset} '
          f'{[m.local_name for m in columns_to_add]}')

    # Add the columns making the best guess of the proper persona
    for m in columns_to_add:
        if debugging_logs:
            print(f'{color.fg_magenta}Metadata Record -> {m.local_name}:{color.reset} {m}')

        persona = get_persona_by_metadata_local_type(m.local_type)
        persona_dict = personas.get(persona, {})
        if debugging_logs:
            print(f'  - {color.fg_blue}Persona -> {persona}:{color.reset} {persona_dict}')

        column = create_column(m.local_name, persona_dict)

        if debugging_logs:
            print(f'  - {color.fg_cyan}Creating Column -> {column.name}:{color.reset} {column.dict()}')
        ds.enforce_column(column, remote_name=m.remote_name)

    return ds

def datasource(args, server=None):
    """ Updates a Tableau Datasource locally

    Args:
        args: An argparse args object
        server (TableauServer): A TableauServer object; optional
    """

    # Set variables from args
    # General Args
    debugging_logs = args.debugging_logs
    include_extract = args.include_extract
    save_tds = args.save_tds

    # Datasource Args
    datasource_path = args.file_path
    datasource_id = args.id
    datasource_name = args.name
    project_name = args.project_name
    location = args.location
    enforce_connection = args.enforce_connection
    empty_extract = args.empty_extract
    filter_extract = args.filter_extract

    # Folder/Fields Args
    persona = args.persona
    delete = args.delete
    column_name = args.column_name
    folder_name = args.folder_name
    caption = args.caption
    desc = args.desc
    calculation = args.calculation
    remote_name = args.remote_name
    list_objects = args.list.title() if args.list else None
    column_init = args.column_init
    clean_folders = args.clean_folders

    # Datasource Connection Args
    conn_type = args.conn_type
    conn_host = args.conn_host
    conn_user = args.conn_user
    conn_role = args.conn_role
    conn_db = args.conn_db
    conn_schema = args.conn_schema
    conn_warehouse = args.conn_warehouse


    # Downloads the datasource from Tableau Server if the datasource is not local
    if location == 'online':
        start = time()
        print(f'{color.fg_cyan}...Downloading {datasource_name}...{color.reset}')
        d = server.get.datasource(datasource_id, datasource_name, project_name)
        datasource_path = server.download.datasource(d.id, include_extract=include_extract)
        print(f'{color.fg_green}{symbol.success}  (Done in {round(time() - start)} sec) '
              f'Downloaded Datasource: {color.fg_yellow}{datasource_path}{color.reset}\n')

    datasource_file_name = os.path.basename(datasource_path)
    ds = Datasource(datasource_path)

    # Add an empty .hyper file to the Datasource; Useful for publishing without data
    if empty_extract:
        from tableau_utilities.hyper.hyper import create_empty_hyper_extract
        create_empty_hyper_extract(ds)
        print(f'{color.fg_green}Added empty .hyper extract for {datasource_path}{color.reset}')
    # Otherwise, filter the extract if filter_extract string provided
    elif filter_extract:
        from tableau_utilities.hyper.hyper import filter_hyper_extract
        start = time()
        print(f'{color.fg_cyan}...Filtering extract data...{color.reset}')
        filter_hyper_extract(ds, filter_extract)
        print(f'{color.fg_green}{symbol.success} (Done in {round(time() - start)} sec) '
              f'Filtered extract data for {datasource_path}{color.reset}')

    if save_tds:
        start = time()
        print(f'{color.fg_cyan}...Extracting {datasource_file_name}...{color.reset}')
        save_folder = f'{datasource_file_name} - BEFORE'
        os.makedirs(save_folder, exist_ok=True)
        if ds.extension == 'tds':
            xml_path = os.path.join(save_folder, datasource_file_name)
            shutil.copy(datasource_path, xml_path)
        else:
            xml_path = ds.unzip(extract_to=save_folder, unzip_all=True)
        if debugging_logs:
            print(f'{color.fg_green}{symbol.success} (Done in {round(time() - start)} sec) '
                  f'BEFORE - TDS SAVED TO: {color.fg_yellow}{xml_path}{color.reset}')

    # List each of the objects specified to list
    if list_objects:
        print(f'{color.fg_cyan}{list_objects}:{color.reset}')
    if list_objects == 'Columns':
        for c in ds.columns:
            print(f'  {symbol.arrow_r} '
                  f'{color.fg_yellow}caption:{color.reset} {c.caption} '
                  f'{color.fg_yellow}local-name:{color.reset} {c.name} '
                  f'{color.fg_yellow}remote-name:{color.reset} {c.name} '
                  f'{color.fg_yellow}persona:{color.reset} {get_persona_by_attribs(c.role, c.type, c.datatype)}')
    if list_objects == 'Folders':
        for f in ds.folders_common.folder:
            print(f'  {symbol.arrow_r} {color.fg_yellow}{f.name}{color.reset}')
    if list_objects == 'Metadata':
        for m in ds.connection.metadata_records:
            print(f'  {symbol.arrow_r} '
                  f'{color.fg_yellow}local-name:{color.reset} {m.local_name} '
                  f'{color.fg_yellow}remote-name:{color.reset} {m.remote_name} '
                  f'{color.fg_yellow}persona:{color.reset} {get_persona_by_metadata_local_type(m.local_type)}')
    if list_objects == 'Connections':
        for c in ds.connection.named_connections:
            print(f'  {symbol.arrow_r} {c.connection.dict()}')

    # Column Init - Add columns for any column in Metadata records but not in columns
    if column_init:
        ds = add_metadata_records_as_columns(ds, color, debugging_logs)

    # Add / modify a specified column
    if column_name and not delete:
        # Column name needs to be enclosed in brackets
        if debugging_logs:
            print('Going to add/update column:', column_name)
        column = ds.columns.get(column_name)
        if persona:
            persona = personas.get(persona.lower(), {})
        else:
            persona = dict()

        if not column:
            if not persona:
                raise Exception('Column does not exist, and more args are need to add a new column.\n'
                                f'Minimum required args: {color.fg_yellow}--column_name --persona{color.reset}')
            column = create_column(column_name, persona)
            print(f'{color.fg_cyan}Creating new column for {column_name}:{color.reset} {column.dict()}')
        else:
            print(f'{color.fg_cyan}Updating existing column:{color.reset}\n  {column.dict()}')

        column.caption = caption or column.caption
        column.role = persona.get('role') or column.role
        column.type = persona.get('role_type') or column.type
        column.datatype = persona.get('datatype') or column.datatype
        column.desc = desc or column.desc
        column.calculation = calculation or column.calculation

        if debugging_logs:
            print(f'{color.fg_yellow}column:{color.reset}{column}')

        ds.enforce_column(column, remote_name=remote_name, folder_name=folder_name)

    # Add a folder if it was specified and does not exist already
    if folder_name and not ds.folders_common.get(folder_name) and not delete:
        if debugging_logs:
            print(f'Going to add folder: {color.fg_cyan}{folder_name}{color.reset}')
        ds.folders_common.add(tfo.Folder(name=folder_name))

    # Delete specified object
    if delete == 'column':
        ds.columns.delete(column_name)
    if delete == 'folder':
        ds.folders_common.folder.delete(folder_name)

    # Clean folders
    if clean_folders:
        cleaned = ds.remove_empty_folders()
        print(f'Removed this list of folders: {color.fg_cyan}{cleaned}{color.reset}')

    # Enforce Connection
    if enforce_connection:
        if debugging_logs:
            print(f'Updating the datasource connection: {color.fg_cyan}{conn_type}{color.reset}')
        connection = ds.connection.get(conn_type)
        if not connection and debugging_logs:
            print(f'Datasource does not contain a connection of type: {conn_type}')
        else:
            connection.class_name = conn_type or connection.class_name
            connection.server = conn_host or connection.server
            connection.username = conn_user or connection.username
            connection.service = conn_role or connection.service
            connection.dbname = conn_db or connection.dbname
            connection.schema = conn_schema or connection.schema
            connection.warehouse = conn_warehouse or connection.warehouse
            ds.connection.update(connection)

    # Save the datasource if an edit may have happened
    if column_name or folder_name or delete or enforce_connection or empty_extract or column_init or clean_folders:
        start = time()
        print(f'{color.fg_cyan}...Saving datasource changes...{color.reset}')
        ds.save()
        print(f'{color.fg_green}{symbol.success} (Done in {round(time() - start)} sec) '
              f'Saved datasource changes: {color.fg_yellow}{datasource_path}{color.reset}')

    if save_tds:
        start = time()
        print(f'{color.fg_cyan}...Extracting {datasource_file_name}...{color.reset}')
        save_folder = f'{datasource_file_name} - AFTER'
        os.makedirs(save_folder, exist_ok=True)
        if ds.extension == 'tds':
            xml_path = os.path.join(save_folder, datasource_file_name)
            shutil.copy(datasource_path, xml_path)
        else:
            xml_path = ds.unzip(extract_to=save_folder, unzip_all=True)
        if debugging_logs:
            print(f'{color.fg_green}{symbol.success} (Done in {round(time() - start)} sec) '
                  f'AFTER - TDS SAVED TO: {color.fg_yellow}{xml_path}{color.reset}')
