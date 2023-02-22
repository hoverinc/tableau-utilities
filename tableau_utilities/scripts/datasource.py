import tableau_utilities.tableau_file.tableau_file_objects as tfo
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
    name = args.name
    project_name = args.project_name

    # Downloads the datasource from Tableau Server if the datasource is not local
    if args.location == 'online':
        # Query Tableau Server for the ID of the datasource if it was not provided
        if args.id:
            datasource_id = args.id
        elif name and project_name:
            datasource_id = server.get_datasource(datasource_name=name, datasource_project=project_name).id
        else:
            raise Exception('For online datasources, id or name and project_name are required.')
        datasource_path = server.download_datasource(datasource_id, args.include_extract)
        if debugging_logs:
            print(f'DATASOURCE PATH: {datasource_path}')

    ds = Datasource(datasource_path)

    if args.save_tds:
        xml_path = ds.unzip(extract_to=f'{name} - BEFORE')
        if debugging_logs:
            print(f'BEFORE - TDS SAVED TO: {xml_path}')

    if args.column:
        # Column name needs to be enclosed in brackets
        column_name = f'[{args.column_name}]'
        column = ds.columns.get(args.column_name)

        if not column:
            column = tfo.Column(name=column_name, datatype=args.datatype, role=args.role, type=args.role_type)
            print(f'Creating new column for {column_name}')
        else:
            print(f'Updating existing column:\n\t{column}')

        column.caption = args.caption or column.caption
        column.role = args.role or column.role
        column.type = args.role_type or column.type
        column.datatype = args.datatype or column.datatype
        column.desc = args.desc or column.desc
        column.calculation = args.calculation or column.calculation
        ds.enforce_column(column, remote_name=args.remote_name, folder_name=args.folder_name)

    if args.folder == 'add':
        ds.folders_common.folder.add(tfo.Folder(name=args.folder_name))
    if args.folder == 'delete':
        ds.folders_common.folder.delete(tfo.Folder(name=args.folder_name))

    ds.save()

    if args.save_tds:
        xml_path = ds.unzip(extract_to=f'{name} - AFTER')
        if debugging_logs:
            print(f'AFTER - TDS SAVED TO: {xml_path}')
