import tableauserverclient as TSC
from xml.etree import ElementTree
import os
import datetime

from settings import tableau_credentials


def make_filter(**kwargs):
    options = TSC.RequestOptions()
    for item, value in kwargs.items():
        name = getattr(TSC.RequestOptions.Field, item)
        options.filter.add(TSC.Filter(name, TSC.RequestOptions.Operator.Equals, value))
    return options


def get_projects(server, print_info=True):
    """ Gets the projects and returns a dictionary with the names and ids

    Args:
        server (obj): A server auth object
        print_info (bool): True to print the dictionary

    Returns: A dictionary with key value pairs of project_name, project_id
    """
    projects = {}

    all_project_items, pagination_item = server.projects.get()
    print([proj.name for proj in all_project_items])

    for project in all_project_items:
        projects[project.name] = project.id

    if print_info:
        for k, v in projects.items():
            print(k, v)

    return projects


def get_datasources(server, print_info=True):
    """ Gets the datasources and returns a dictionary with the names and ids

    Args:
        server (obj): A server auth object
        print_info (bool): True to print the dictionary

    Returns: A dictionary with k, v pairs of datasource_name, datasource_id
    """
    datasources = {}

    all_datasources, pagination_item = server.datasources.get()
    print(f"\nThere are {pagination_item.total_available} datasources on site: ")

    for datasource in all_datasources:
        datasources[datasource.name] = datasource.id

    if print_info:
        for k, v in datasources.items():
            print(k, v)

    return datasources


def get_schedules(server, schedule_type='Extract', print_info=True):
    """ Gets the schedules and returns a dictionary with the names and ids

    Args:
        server (obj): A server auth object
        schedule_type (str): "Extract" or "Scubscription"
        print_info (bool): True to print the dictionary

    Returns: A dictionary with k, v pairs of schedule_name, schedule_id
    """

    schedules = {}

    all_schedules = [x for x in TSC.Pager(server.schedules) if x.schedule_type == schedule_type]

    for schedule in all_schedules:
        schedules[schedule.name] = schedule.id

    if print_info:
        for k, v in schedules.items():
            print(k, v)

    return schedules


def get_refresh_extract_tasks(server, task_type=None, print_info=True):
    """  Gets the refresh extract tasks for datasources and workbooks

    Args:
        server (obj): A server auth object
        task_type (str): Specify workbook or datasource to limit the types
        print_info (bool): True to print the dictionary

    Returns: A dictionary with the where key is the tasks id and value is a dictionary with
        task_type, priority, consecutive_failed_count, schedule_id, target_id and target_type
    """

    tasks = {}

    all_tasks, pagination = server.tasks.get()

    for task in all_tasks:
        if task_type and task.task_type != task_type:
            continue

        # tasks[task.id] = {'priority': task.pri}
        tasks[task.id] = {
            'task_type': task.task_type,
            'priority': task.priority,
            'consecutive_failed_count': task.consecutive_failed_count,
            'schedule_id': task.schedule_id,
            'target_id': task.target.id,
            'target_type': task.target.type
        }

        if print_info:
            print(task)

    return tasks


def download_datasource(server, datasource_id, print_info=True):
    """ Downloads datasource by ID

    Args:
        server (obj): A server auth object
        datasource_id (str): The ID of the datasource to download
        print_info (bool): True to print the dictionary
    """
    file_path = server.datasources.download(datasource_id)

    if print_info:
        print(f"\nDownloaded datasource {datasource_id} to:\n\t{file_path}")


def publish_datasource(server, project_id, file_path, mode):
    """ Publishes a datasource to Tableau Server

    Args:
        server (obj): A server auth object
        project_id (str): The ID of the project to publish the datasource to
        file_path (str): The path to the datasource file. Must be a .tds, tdsx, .tde, or .hyper file
        mode (str): CreateNew, Overwrite, or Append
    """

    # Use the project id to create new datsource_item
    new_datasource = TSC.DatasourceItem(project_id)

    # publish data source (specified in file_path)
    new_datasource = server.datasources.publish(new_datasource, file_path, mode)


def kill_jobs(server):
    """ Kills all currently running jobs on Tableau server

    Args:
        server (obj): A server auth object
    """

    req = TSC.RequestOptions()

    req.filter.add(TSC.Filter("progress", TSC.RequestOptions.Operator.LessThanOrEqual, 0))

    for job in TSC.Pager(server.jobs, request_opts=req):
        print(server.jobs.cancel(job.id), job.id, job.status, job.type)


def get_datasource_by_name(server, name) -> str:
    """ Gets the datasource by name and returns the ID

    Args:
        server (obj): A server auth object
        name (str): The name of the datasource to retrieve

    Returns (str): The ID for the datasources
    """
    request_filter = make_filter(Name=name)
    datasources, _ = server.datasources.get(request_filter)
    assert len(datasources) == 1
    data_source = datasources.pop()
    return data_source.id


def download_all_workbooks(server, save_dir, print_info=True):
    """ Downloads all workbooks from the server and sorts them into folders based on the workbooks project name.
        The main folder will be given a timestamp based on the date the function is run.

    Args:
        server (obj): A server auth object
        save_dir (str): The directory to save the workbooks to
        print_info (bool): False not to print the path to the workbooks being downloaded
    """
    today_date = str(datetime.datetime.today().date())
    for workbook in TSC.Pager(server.workbooks):
        save_path = os.path.join(save_dir, today_date, workbook.project_name)
        try:
            os.makedirs(save_path)
        except FileExistsError:
            pass
        finally:
            file_path = server.workbooks.download(workbook_id=workbook.id, filepath=save_path)
            if print_info:
                print(f"\nDownloaded workbook {workbook.id} to:\n\t{file_path}")


def main():
    tableau_auth = TSC.TableauAuth(
        username=tableau_credentials["username"],
        password=tableau_credentials["password"],
        site_id=tableau_credentials["site_id"]
    )
    server = TSC.Server(tableau_credentials["server_url"], use_server_version=3.1)

    with server.auth.sign_in(tableau_auth):
        # projects = get_projects(server, print_info=True)
        # datasources = get_datasources(server, print_info=True)
        # schedules = get_schedules(server, print_info=True)
        # tasks = get_refresh_extract_tasks(server, print_info=True)

        # download_datasource(server, datasource_id='abc-123')
        # publish_datasource(server, project_id='abc-123', file_path='datasource.tdsx', mode='Overwrite')

        # datasource_id = get_datasource_by_name(server, 'Jobs')
        download_all_workbooks(server, 'workbooks')


if __name__ == "__main__":
    main()

