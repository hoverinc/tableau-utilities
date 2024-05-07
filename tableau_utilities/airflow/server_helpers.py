def get_tableau_server(tableau_conn_id: str):
    """ Returns a TableauServer object """
    conn = BaseHook.get_connection(tableau_conn_id)
    api_version = conn.extra_dejson.get('api_version')
    if api_version:
        api_version = float(api_version)
    return TableauServer(
        host=conn.host
        , site=conn.extra_dejson.get('site')
        , api_version=api_version
        , personal_access_token_name=conn.extra_dejson.get('personal_access_token_name')
        , personal_access_token_secret=conn.extra_dejson.get('personal_access_token_secret')
    )


def refresh_datasources(tasks, tableau_conn_id='tableau_default'):
    """ Refresh a datasource extract.

    Args:
        tasks (str|dict): A dictionary of the actions for updating the datasource.
        tableau_conn_id (str): The Tableau connection ID
    """
    if isinstance(tasks, str):
        tasks: dict = ast.literal_eval(tasks)
    ts = get_tableau_server(tableau_conn_id)

    for datasource_id in tasks:
        datasource_name = tasks[datasource_id]['datasource_name']
        # All listed datasources in this variable won't be refreshed
        # Common use-case for not refreshing a datasource, is because it has a live connection
        if datasource_name in SKIP_REFRESH:
            logging.info('(SKIP) Marked to skip refresh: %s %s', datasource_id, datasource_name)
            continue

        try:
            ts.refresh.datasource(datasource_id)
            logging.info('Refreshed: %s %s', datasource_id, datasource_name)
        except Exception as error:
            if 'Not queuing a duplicate.' in str(error):
                logging.info(error)
                logging.info('(SKIP) Refresh already running: %s %s',
                             datasource_id, datasource_name)
            else:
                raise Exception(error) from error
