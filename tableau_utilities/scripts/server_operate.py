from tableau_utilities.tableau_server.tableau_server import TableauServer


def server_operate(args, server):
    """ Allows downloading, publishing, and refreshing workbooks and datasources

    Args:
        args: The args from the cli
        server (TableauServer): the Tableau Server authentication object

    """

    # Set the inputs from the args.
    object_type = args.object_type.lower() if args.object_type else ''
    action_type = args.action_type
    include_extract = args.include_extract
    # These might be none
    object_id = args.id
    object_name = args.name
    project_name = args.project_name
    all_objects = args.all

    if object_type not in ['workbook', 'datasource']:
        raise Exception('Please provide object_type as either "workbook" or "datasource"')

    if all_objects and action_type == 'download':
        object_list = [o for o in getattr(server, f'get_{object_type}s')()]
        for o in object_list:
            print(f"DOWNLOADING {object_type}: {o.name} {o.project_name} {o.id}")
            response = getattr(server, f'download_{object_type}')(o['id'], include_extract)
            print(f'RESPONSE {response}')
    else:
        # Gets the ID, name, and project from the object in Tableau Server
        obj = getattr(server, f'get_{object_type}')(object_id, object_name, project_name)
        object_id = obj.id or object_id
        object_name = obj.name or object_name
        project_name = obj.project_name or project_name

        if action_type == 'download':
            print(
                f'GETTING {object_type.upper()} -> ID: {object_id}, '
                f'NAME: {object_name}, '
                f'PROJECT: {project_name}, '
                f'INCLUDE EXTRACT {include_extract}'
            )
            getattr(server, f'download_{object_type}')(object_id, include_extract)
        elif action_type == 'publish':
            print(
                f'PUBLISHING {object_type.upper()} -> ID: {object_id}, '
                f'OBJECT NAME: {object_name}, '
                f'PROJECT NAME: {project_name}'
            )
            response = getattr(server, f'publish_{object_type}')(args.file_path, object_id, object_name, project_name)
            print(f'RESPONSE {response}')
        elif action_type == 'refresh':
            print(
                f'REFRESHING {object_type.upper()} -> ID: {object_id}, '
                f'OBJECT NAME: {object_name}, '
                f'PROJECT NAME: {project_name}'
            )
            response = getattr(server, f'refresh_{object_type}')(object_id)
            print(f'RESPONSE {response}')