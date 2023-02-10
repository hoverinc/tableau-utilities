import os

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_file.tableau_file import TableauFile
from tableau_utilities.tableau_file.tableau_file_objects import Column
from tableau_utilities.scripts.server_operate import fill_in_id_name_project, get_object_list


def save_datasource(server, id, datasource_name, project_name, include_extract, debugging_logs):
    """ Gets the id if needed and downloads the datasource

    Args:
        id: The datasource id
        datasource_name: The name of the datasource
        project_name: The name of the project the datasource is in
        include_extract: True to include the extract. Downloading may take a long time
        debugging_logs: Print information for debugging to the console


    """

    object_list = get_object_list(object_type='datasource', server=server)
    id, datasource_name, project_name = fill_in_id_name_project(id, datasource_name,
                                                                           project_name, object_list)
    if debugging_logs:
        print(
            f'GETTING DATASOURCE ID: {id}, NAME: {datasource_name}, PROJECT NAME: {project_name}, INCLUDE EXTRACT {include_extract}')

    datasource_path = server.download_datasource(id, include_extract)

    if debugging_logs:
        print(
            f'DATASOURCE PATH: {datasource_path}')

    return datasource_path


def save_tds_file(datasource_path, extract_to_directory):
    """ Extracts XML from a tdsx file and saves as a raw .xml file

    """

    # xml_path = TableauFile(datasource_path).save(save_raw_xml=True)
    xml_path = TableauFile(datasource_path).unzip(extract_to=extract_to_directory)
    print(f'TDS SAVED TO: {xml_path}')


def datasource(args, server=None):

    debugging_logs = args.debugging_logs

    location = args.location
    datasource_path = args.file_path
    id = args.id
    name = args.name
    project_name = args.project_name
    include_extract = args.include_extract
    # extract_to_directory = args.local_folder
    extract_to_directory =  os.getcwd()
    save_tds = args.save_tds

    if location == 'online':
        datasource_path = save_datasource(server, id, name, project_name, include_extract, debugging_logs)

    if args.save_tds:
        save_tds_file(datasource_path, extract_to_directory)

    if datasource_path is not None:
        datasource = Datasource(datasource_path)

    if args.column:
        # Column name needs to be enclosed in brackets
        column_name = f'[{args.column_name}]'

        column = datasource.columns.get(args.column_name)
        print(column)

        if not column:
            column = Column(name=column_name, datatype=args.datatype, role=args.role, type=args.role_type)

        column.name = column_name or column.name
        column.caption = args.caption or column.caption
        column.role = args.role or column.role
        column.type = args.role_type or column.type
        column.datatype = args.datatype or column.datatype
        column.desc = args.desc or column.desc
        column.calculation = args.calculation or column.calculation


        datasource.enforce_column(column, remote_name=args.remote_name, folder_name=args.folder_name)
        datasource.save()
        datasource.unzip()

    if args.folder == 'add':
        datasource.folders_common.folder.add(tfo.Folder(name=args.folder_name))
    if args.folder == 'delete':
        datasource.folders_common.folder.delete(tfo.Folder(name=args.folder_name))


