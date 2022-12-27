
from tableau_utilities.scripts.server_info import object_list_to_dicts


def get_object_id(project_name, object_name, object_list):
    """ Gets the object ID from a Object Name and Project Name

    Args:
        project_name: The name of the project the object is in
        object_name: The name of the workbook or datasource
        object_list: The list of datasource or workbook objects

    Returns:
          id: The ID for the object

    """

    return [o['id'] for o in object_list if o['name'] == object_name and  o['project_name'] == project_name][0]


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


def download_objects(args, server):

    if args.object_type == 'datasource':
        object_list = [d for d in server.get_datasources()]
    if args.object_type == 'workbook':
        object_list = [w for w in server.get_workbooks()]

    object_list = object_list_to_dicts(object_list)

    id = args.id
    object_name = args.name
    project_name = args.project_name

    if id is not None:
        project_name, object_name = get_project_and_object_names(id, object_list)

    if id is None:
        id = get_object_id(project_name, object_name, object_list)

    print(f'GETTING OBJECT ID: {id}, OBJECT NAME: {object_name}, PROJECT NAME: {project_name}, INCLUDE EXTRACT {args.include_extract}')

    if args.object_type == 'datasource':
        server.download_datasource(id, include_extract=args.include_extract)
    if args.object_type == 'workbook':
        server.download_workbook(id, include_extract=args.include_extract)

