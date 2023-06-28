import webbrowser

from tableau_utilities.general.cli_styling import Color, Symbol, color_print
from tableau_utilities.tableau_server.tableau_server import TableauServer
from tableau_utilities.tableau_server.tableau_server_objects import Datasource, Workbook, Job, Connection


def server_operate(args, server):
    """ Allows downloading, publishing, and refreshing workbooks and datasources

    Args:
        args: The args from the cli
        server (TableauServer): the Tableau Server authentication object

    """

    # Set variables from args
    # Required -> One must be provided
    object_type = args.download or args.publish or args.refresh
    if args.embed_connection:
        object_type = 'datasource'
    # Optional -> Might be None
    include_extract = args.include_extract
    object_id = args.id
    object_name = args.name
    project_name = args.project_name
    all_objects = args.all
    download = args.download
    publish = args.publish
    embed_connection = args.embed_connection
    refresh = args.refresh
    file_path = args.file_path
    connection = None
    if object_type == 'datasource' and args.conn_user and args.conn_pw:
        connection = {'username': args.conn_user, 'password': args.conn_pw}

    # Print Styling
    color = Color()
    symbol = Symbol()

    # Download all objects, and return early if all objects have been downloaded
    if all_objects and download:
        object_list = [o for o in getattr(server, f'get_{object_type}s')()]
        for o in object_list:
            print(
                f'{color.fg_yellow}DOWNLOADING {object_type} {symbol.arrow_r} {color.fg_grey}'
                f'ID: {o.id} {symbol.sep} '
                f'NAME: {o.name} {symbol.sep} '
                f'PROJECT: {o.project_name} {symbol.sep} '
                f'INCLUDE EXTRACT: {include_extract}{color.reset}'
            )
            res = getattr(server, f'download_{object_type}')(o.id, include_extract=include_extract)
            color_print(f'{symbol.success}  {res}', fg='green')
        return f'Successfully downloaded all {object_type}s'

    # Gets the ID, name, and project from the object in Tableau Server
    obj = getattr(server, f'get_{object_type}')(object_id, object_name, project_name)
    object_id = obj.id or object_id
    object_name = obj.name or object_name
    project_name = obj.project_name or project_name

    if download:
        print(
            f'{color.fg_yellow}DOWNLOADING {object_type.upper()} {symbol.arrow_r} {color.fg_grey}'
            f'ID: {object_id} {symbol.sep} '
            f'NAME: {object_name} {symbol.sep} '
            f'PROJECT: {project_name} {symbol.sep} '
            f'INCLUDE EXTRACT: {include_extract}{color.reset}'
        )
        res: str = getattr(server, f'download_{object_type}')(object_id, include_extract=include_extract)
        color_print(f'{symbol.success}  {res}', fg='green')
    elif publish:
        print(
            f'{color.fg_yellow}PUBLISHING {object_type.upper()} {symbol.arrow_r} {color.fg_grey}'
            f'ID: {object_id} {symbol.sep} '
            f'NAME: {object_name} {symbol.sep} '
            f'PROJECT NAME: {project_name}{color.reset}'
        )
        res: Datasource | Workbook = getattr(server, f'publish_{object_type}')(
            file_path, object_id, object_name, project_name,
            connection=connection
        )
        color_print(f'{symbol.success}  {project_name} / {object_name}:', fg='green')
        color_print(f'  {symbol.arrow_r} {res.webpage_url}', fg='cyan')
        # Open URL to the published datasource in the browser
        webbrowser.open(res.webpage_url)
    elif embed_connection:
        print(
            f'{color.fg_yellow}EMBEDDING DATASOURCE CONNECTION CREDS {symbol.arrow_r} {color.fg_grey}'
            f'ID: {object_id} {symbol.sep} '
            f'NAME: {object_name} {symbol.sep} '
            f'PROJECT NAME: {project_name}{color.reset}'
        )
        res: Connection = server.embed_datasource_credentials(object_id, connection, args.conn_type)
        color_print(f'{symbol.success}  {res}', fg='green')
    elif refresh:
        print(
            f'{color.fg_yellow}REFRESHING {object_type.upper()} {symbol.arrow_r} {color.fg_grey}'
            f'ID: {object_id} {symbol.sep} '
            f'NAME: {object_name} {symbol.sep} '
            f'PROJECT NAME: {project_name}{color.reset}'
        )
        res: Job = getattr(server, f'refresh_{object_type}')(object_id)
        color_print(f'{symbol.success}  {res}', fg='green')
