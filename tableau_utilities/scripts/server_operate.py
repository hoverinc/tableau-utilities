from tableau_utilities.general.cli_styling import Color, Symbol
from tableau_utilities.tableau_server.tableau_server import TableauServer


def server_operate(args, server):
    """ Allows downloading, publishing, and refreshing workbooks and datasources

    Args:
        args: The args from the cli
        server (TableauServer): the Tableau Server authentication object

    """
    # Required -> One must be provided
    object_type = args.download or args.publish or args.refresh
    # Optional -> Might be None
    include_extract = args.include_extract
    object_id = args.id
    object_name = args.name
    project_name = args.project_name
    all_objects = args.all
    # Print Styling
    color = Color()
    symbol = Symbol()

    # Download all objects, and return early if all objects have been downloaded
    if all_objects and args.download:
        object_list = [o for o in getattr(server, f'get_{object_type}s')()]
        for o in object_list:
            print(
                f'{color.fg_yellow}DOWNLOADING {object_type} {symbol.arrow_r} {color.fg_grey}'
                f'ID: {o.id} {symbol.sep} '
                f'NAME: {o.name} {symbol.sep} '
                f'PROJECT: {o.project_name} {symbol.sep} '
                f'INCLUDE EXTRACT: {include_extract}{color.reset}'
            )
            response = getattr(server, f'download_{object_type}')(o.id, include_extract=include_extract)
            print(f'{color.fg_green}{symbol.success}  {response}{color.reset}')
        return f'Successfully downloaded all {object_type}s'

    # Gets the ID, name, and project from the object in Tableau Server
    obj = getattr(server, f'get_{object_type}')(object_id, object_name, project_name)
    object_id = obj.id or object_id
    object_name = obj.name or object_name
    project_name = obj.project_name or project_name

    if args.download:
        print(
            f'{color.fg_yellow}DOWNLOADING {object_type.upper()} {symbol.arrow_r} {color.fg_grey}'
            f'ID: {object_id} {symbol.sep} '
            f'NAME: {object_name} {symbol.sep} '
            f'PROJECT: {project_name} {symbol.sep} '
            f'INCLUDE EXTRACT: {include_extract}{color.reset}'
        )
        response = getattr(server, f'download_{object_type}')(object_id, include_extract=include_extract)
        print(f'{color.fg_green}{symbol.success}  {response}{color.reset}')
    elif args.publish:
        print(
            f'{color.fg_yellow}PUBLISHING {object_type.upper()} {symbol.arrow_r} {color.fg_grey}'
            f'ID: {object_id} {symbol.sep} '
            f'NAME: {object_name} {symbol.sep} '
            f'PROJECT NAME: {project_name}{color.reset}'
        )
        response = getattr(server, f'publish_{object_type}')(args.file_path, object_id, object_name, project_name)
        print(f'{color.fg_green}{symbol.success}  {response}{color.reset}')
    elif args.refresh:
        print(
            f'{color.fg_yellow}REFRESHING {object_type.upper()} {symbol.arrow_r} {color.fg_grey}'
            f'ID: {object_id} {symbol.sep} '
            f'NAME: {object_name} {symbol.sep} '
            f'PROJECT NAME: {project_name}{color.reset}'
        )
        response = getattr(server, f'refresh_{object_type}')(object_id)
        print(f'{color.fg_green}{symbol.success}  {response}{color.reset}')
