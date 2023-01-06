from tableau_utilities.scripts.server_info import object_list_to_dicts


def get_object_id(project_name, object_name, object_list):
    """ Gets the object ID from a Object Name and Project Name

    Args:
        project_name: The name of the project the object is in
        object_name: The name of the workbook or datasource
        object_list: The list of datasource or workbook objects

    Returns:
          id: The ID for the object if it exists

    """

    try:
        id = [o['id'] for o in object_list if o['name'] == object_name and  o['project_name'] == project_name][0]
    except:
        id = None

    return id


def get_project_and_object_names(id, object_list):
    """ Gets the project_name and object_name for an ID

    Args:
        id: The id of the workbook or datasource
        object_list: The list of datasource or workbook objects

    Returns:
        name: The name of the object
        project_name: The name of the project containing the item


    """

    for o in object_list:
        if o['id'] == id:
            return o['name'], o['project_name']


def get_object_list(object_type, server):
    """ Gets a dictionary with the list of objects for a type

    Args:
        object_type: datasource or workbook
        server: the tableau server authentication object

    """

    if object_type == 'datasource':
        object_list = [d for d in server.get_datasources()]
    if object_type == 'workbook':
        object_list = [w for w in server.get_workbooks()]

    object_list = object_list_to_dicts(object_list)
    return object_list


def fill_in_id_name_project(id, object_name, project_name, object_list):
    """ Makes sure that the name, object_name, and project_name all have values and are not None

    Args:
        id: The id of the object
        object_name: The name of the object
        project_name: The name of the project the object is in
        object_list: The list of objects

    Returns:
        id, object_name, project_name without a value of none for any

    """

    if id is not None:
        project_name, object_name = get_project_and_object_names(id, object_list)

    if id is None:
        id = get_object_id(project_name, object_name, object_list)

    # TO DO: Add error message
    # if id is None or object_name is None or project_name is None:
    #     raise

    return id, object_name, project_name


def server_operate(args, server):
    """ Allows downloading, publishing, and refreshing workbooks and datasources

    Args:
        args: The args from the cli
        server: the Tableau Server authentication object

    """

    # Set the inputs from the args.
    object_type = args.object_type
    action_type = args.action_type
    include_extract = args.include_extract

    # These might be none
    id = args.id
    object_name = args.name
    project_name = args.project_name

    if args.all and action_type == 'download':
        object_list = get_object_list(object_type, server)
        # object_list = object_list_to_dicts(object_list)
        for o in object_list:
            print(f"DOWNLOADING {o['name']} {o['project_name']} {o['id']}")
            response = server.download_datasource(o['id'], include_extract=include_extract)
            print(response)

    else:
        object_list = get_object_list(object_type=args.object_type, server=server)
        id, object_name, project_name = fill_in_id_name_project(id, object_name, project_name, object_list)

        if action_type == 'download':
            print(
                f'GETTING OBJECT ID: {id}, OBJECT NAME: {object_name}, PROJECT NAME: {project_name}, INCLUDE EXTRACT {include_extract}')
            if args.object_type == 'datasource':
                server.download_datasource(id, include_extract=include_extract)
            if args.object_type == 'workbook':
                server.download_workbook(id, include_extract=include_extract)
        elif action_type == 'publish':
            print(f'PUBLISHING ID: {id}, OBJECT NAME: {object_name}, PROJECT NAME: {project_name}')
            if args.object_type == 'datasource':
                response = server.publish_datasource(args.file_path, datasource_id=id, datasource_name=object_name, project_name=project_name)
            if args.object_type == 'workbook':
                response = server.publish_workbook(args.file_path, workbook_id=id, workbook_name=object_name, project_name=project_name)
            print(f'RESPONSE {response}')
        elif action_type == 'refresh':
            print(f'REFRESHING ID: {id}, OBJECT NAME: {object_name}, PROJECT NAME: {project_name}')
            if args.object_type == 'datasource':
                response = server.refresh_datasource(datasource_id=id)
            if args.object_type == 'workbook':
                response = server.refresh_workbook(workbook_id=id)
            print(f'RESPONSE {response}')
