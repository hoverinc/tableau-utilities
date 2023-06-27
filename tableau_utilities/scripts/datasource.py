import os

import tableau_utilities.tableau_file.tableau_file_objects as tfo
from tableau_utilities.general.config_column_persona import personas, get_persona_by_attribs, \
    get_persona_by_metadata_local_type
from tableau_utilities.general.cli_styling import Color
from tableau_utilities.general.cli_styling import Symbol
from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def datasource(args, server=None):
    """ Updates a Tableau Datasource locally

    Args:
        args: An argparse args object
        server (TableauServer): A TableauServer object; optional
    """
    debugging_logs = args.debugging_logs
    datasource_path = args.file_path
    list_objects = args.list.title() if args.list else None
    color = Color()
    symbol = Symbol()

    # Downloads the datasource from Tableau Server if the datasource is not local
    if args.location == 'online':
        # Query Tableau Server for the datasource, use name/project if id was not provided
        if not (args.id or args.name is not None and args.project_name is not None):
            raise Exception('Cannot find datasource. Make sure --id or --name and --project_name are provided.')
        d = server.get_datasource(args.id, args.name, args.project_name)
        datasource_path = server.download_datasource(d.id, args.include_extract)
        print(f'{color.fg_green}{symbol.success}  Downloaded Datasource:', f'{color.fg_yellow}{datasource_path}{color.reset}', '\n')

    datasource_file_name = os.path.basename(datasource_path)
    ds = Datasource(datasource_path)

    if args.save_tds:
        xml_path = ds.unzip(extract_to=f'{datasource_file_name} - BEFORE')
        if debugging_logs:
            print(f'{color.fg_green}{symbol.success}  BEFORE - TDS SAVED TO: {color.fg_yellow}{xml_path}{color.reset}')

    # List each of the objects specified to list
    if list_objects:
        print(f'{color.fg_cyan}{list_objects}:{color.reset}')
    if list_objects == 'Columns':
        for c in ds.columns:
            print(f'  {symbol.arrow_r} '
                  f'{color.fg_yellow}caption:{color.reset} {c.caption} '
                  f'{color.fg_yellow}local-name:{color.reset} {c.name} '
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

    # Add / modify a specified column
    if args.column_name and not args.delete:
        # Column name needs to be enclosed in brackets
        if debugging_logs:
            print('Going to add/update column:', args.column_name)
        column_name = f'[{args.column_name}]'
        column = ds.columns.get(args.column_name)
        persona = dict()
        if args.persona:
            persona = personas.get(args.persona.lower(), {})

        if not column:
            if not args.persona:
                raise Exception('Column does not exist, and more args are need to add a new column.\n'
                                f'Minimum required args: {color.fg_yellow}--column_name --persona{color.reset}')
            column = tfo.Column(
                name=column_name,
                role=persona['role'],
                datatype=persona['datatype'],
                type=persona['role_type'],
            )
            print(f'{color.fg_cyan}Creating new column for {column_name}:{color.reset} {column.dict()}')
        else:
            print(f'{color.fg_cyan}Updating existing column:{color.reset}\n  {column.dict()}')

        column.caption = args.caption or column.caption
        column.role = persona.get('role') or column.role
        column.type = persona.get('role_type') or column.type
        column.datatype = persona.get('datatype') or column.datatype
        column.desc = args.desc or column.desc
        column.calculation = args.calculation or column.calculation
        ds.enforce_column(column, remote_name=args.remote_name, folder_name=args.folder_name)

    # Add a folder if it was specified and does not exist already
    if args.folder_name and not ds.folders_common.get(args.folder_name) and not args.delete:
        if debugging_logs:
            print(f'Going to add folder: {color.fg_cyan}{args.folder_name}{color.reset}')
        ds.folders_common.add(tfo.Folder(name=args.folder_name))

    # Delete specified object
    if args.delete == 'column':
        ds.columns.delete(args.column_name)
    if args.delete == 'folder':
        ds.folders_common.folder.delete(args.folder_name)

    # Save the datasource if an edit may have happened
    if args.column_name or args.folder_name or args.delete:
        ds.save()
        print(f'{color.fg_green}{symbol.success}  Saved changes to: {color.fg_yellow}{datasource_path}{color.reset}')

    if args.save_tds:
        xml_path = ds.unzip(extract_to=f'{datasource_file_name} - AFTER')
        if debugging_logs:
            print(f'{color.fg_green}{symbol.success}  AFTER - TDS SAVED TO: {color.fg_yellow}{xml_path}{color.reset}')
