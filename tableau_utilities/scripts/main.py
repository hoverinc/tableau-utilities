import yaml
import argparse
import tableau_utilities as tu
import tableau_utilities.tableau_file.tableau_file_objects as tfo


def do_args(argv=None):
    """ Parse arguments.

    Args:
        argv (list): an array arguments ['--server', 'myserver',...]

    Returns: an argparse.Namespace
    """

    # parser = argparse.ArgumentParser(
    #     description='Interact with Tableau objects')
    # parser.add_argument(
    #     '--settings_path',
    #     help='Path to your local settings.yaml file (See sample_settings.yaml)',
    #     default=None
    # )
    # parser.add_argument(
    #     '--server',
    #     help='Tableau Server address. i.e. <server_address> in https://<server_address>.online.tableau.com',
    #     default=None
    # )
    # parser.add_argument(
    #     '--site',
    #     help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
    #     default=None
    # )
    # parser.add_argument(
    #     '--api_version',
    #     help='Tableau API version',
    #     default='3.16'
    # )
    # parser.add_argument('--user', default=None, help='Tableau username')
    # parser.add_argument('--password', default=None, help='Tableau password')
    parser.add_argument(
        '--conn_user',
        help='Username for embed credentials. See --embed_creds.',
        default=False
    )
    parser.add_argument(
        '--conn_pw',
        help='Password for embed credentials. See --embed_creds',
        default=False
    )
    parser.add_argument(
        '--conn_type',
        help='Connection type for embed credentials. See --embed_creds',
        default=False
    )
    parser.add_argument('--conn_db', default=None, help='Connection Database')
    parser.add_argument('--conn_schema', default=None, help='Connection Schema')
    parser.add_argument('--conn_host', default=None, help='Connection Host (URL)')
    parser.add_argument('--conn_role', default=None, help='Connection Role')
    parser.add_argument('--conn_warehouse', default=None, help='Connection Warehouse')
    # parser.add_argument(
    #     '--list_datasources',
    #     action='store_true',
    #     default=False,
    #     help='Print the datasource names and ids'
    # )
    # parser.add_argument(
    #     '--list_workbooks',
    #     action='store_true',
    #     default=False,
    #     help='Print the workbook names and ids'
    # )
    parser.add_argument(
        '--update_connection',
        action='store_true',
        default=False,
        help='Update the connection information in the datasource'
    )
    parser.add_argument(
        '--embed_creds',
        action='store_true',
        default=False,
        help='Embed credentials for the datasource'
             ' indicated by datasource ID (--dsid) if supplied, or by --dsn and --project.'
             ' Provide username (--cred_user), password (--cred_pw), and connection type (--conn_type) to embed.'
    )
    # parser.add_argument(
    #     '--id',
    #     default=None,
    #     help='The ID of the datasource or workbook. See --list_datasources'
    # )
    # parser.add_argument('--name', default=None, help='The datasource or workbook name')
    # parser.add_argument(
    #     '--project',
    #     default=None,
    #     help='The project name for the datasource or workbook'
    # )
    # parser.add_argument(
    #     '--download_ds',
    #     default=False,
    #     action='store_true',
    #     help='Download the datasource indicated by datasource ID (--id) if supplied, or by --name and --project.'
    # )
    # parser.add_argument(
    #     '--download_wb',
    #     default=False,
    #     action='store_true',
    #     help='Download the workbook indicated by workbook ID (--id) if supplied, or by --name and --project.'
    # )
    parser.add_argument(
        '--refresh',
        default=False,
        action='store_true',
        help='Refresh the datasource indicated by datasource ID (--dsid) if supplied, or by --dsn and --project.'
    )
    # parser.add_argument(
    #     '--tdsx',
    #     default=None,
    #     help='Path to the tdsx file. See --modify_column and --publish'
    # )
    parser.add_argument(
        '--publish',
        default=False,
        action='store_true',
        help='Publish a datasource. Supply filename in --tdsx and ID in --id'
    )
    parser.add_argument(
        '--add_column',
        default=False,
        action='store_true',
        help='Add the column identified by --column_name in the downloaded datasource. See --tdsx'
    )
    parser.add_argument(
        '--modify_column',
        default=False,
        action='store_true',
        help='Change the column identified by --column_name in the downloaded datasource. See --tdsx'
    )
    parser.add_argument(
        '--add_folder',
        default=False,
        action='store_true',
        help='Add the folder identified by --folder_name in the downloaded datasource. See --tdsx'
    )
    parser.add_argument(
        '--delete_folder',
        default=False,
        action='store_true',
        help='Delete the folder identified by --folder_name in the downloaded datasource. See --tdsx'
    )
    parser.add_argument(
        '--column_name',
        help='The name of the column. See --add_column, and --modify_column'
    )
    parser.add_argument(
        '--remote_name',
        help='The remote (SQL) name of the column. See --add_column, and --modify_column'
    )
    parser.add_argument('--caption', help='Short name/Alias for the column')
    parser.add_argument(
        '--title_case_caption',
        default=False,
        action='store_true',
        help='Converts caption to title case. Applied after --caption'
    )
    parser.add_argument('--role', default=None, choices=['measure', 'dimension'])
    parser.add_argument('--desc', help='A Tableau column description')
    parser.add_argument('--calculation', help='A Tableau calculation')
    parser.add_argument(
        '--datatype',
        default=None,
        choices=['date', 'datetime', 'integer', 'real', 'string']
    )
    parser.add_argument(
        '--role_type',
        default=None,
        choices=['nominal', 'ordinal', 'quantitative']
    )
    parser.add_argument(
        '--folder_name',
        default=None,
        help='The name of the folder. See --add_column, --modify_column, --add_folder and --delete_folder'
    )
    if argv:
        return parser.parse_args(argv)
    else:
        return parser.parse_args()


def main():
    args = do_args()
    tdsx = args.tdsx
    settings = dict()
    if args.settings_path:
        with open(args.settings_path, 'r') as f:
            settings = yaml.safe_load(f)
    else:
        settings['tableau_login'] = {
            'host': f'https://{args.server}.online.tableau.com',
            'site': args.site,
            'api_version': args.api_version,
            'user': args.user,
            'password': args.password
        }
        settings['embed_connection'] = {
            'class_name': args.conn_type,
            'server': args.conn_host,
            'username': args.conn_user,
            'password': args.conn_pw,
            'service': args.conn_role,
            'dbname': args.conn_db,
            'schema': args.conn_schema,
            'warehouse': args.conn_warehouse
        }

    # needs_tableau_server = (
    #     args.list_datasources
    #     or args.list_workbooks
    #     or args.download_ds
    #     or args.download_wb
    #     or args.publish
    #     or args.embed_creds
    #     or args.refresh
    # )

    # if needs_tableau_server:
    #     if not args.settings_path:
    #         missing_creds = dict()
    #         for _arg in ['user', 'password', 'site', 'server']:
    #             if not args.__getattribute__(_arg):
    #                 missing_creds.setdefault('missing', [])
    #                 missing_creds['missing'].append(f'--{_arg}')
    #         if missing_creds:
    #             raise tu.TableauConnectionError(missing_creds)
    #     server = tu.TableauServer(**settings['tableau_login'])
    # if args.list_datasources:
    #     for d in server.get_datasources():
    #         print(d.id, '::', d.name, '::', d.project_name)
    # if args.list_workbooks:
    #     for w in server.get_workbooks():
    #         print(w.id, '::', w.name, w.project_name)
    # if args.download_ds:
    #     tdsx = server.download_datasource(args.id, include_extract=False)
    #     print(f'Downloaded to {tdsx}')
    # if tdsx:
    #     datasource = tu.Datasource(tdsx)
    # if args.download_wb:
    #     server.download_workbook(args.id, include_extract=False)
    if args.add_column or args.modify_column:
        column = tfo.Column(
                name=args.column_name,
                caption=args.caption,
                role=args.role,
                type=args.role_type,
                datatype=args.datatype,
                desc=args.desc,
                calculation=args.calculation
            )
        datasource.enforce_column(column, remote_name=args.remote_name, folder_name=args.folder_name)
    if args.add_folder:
        datasource.folders_common.folder.add(tfo.Folder(name=args.folder_name))
    if args.delete_folder:
        datasource.folders_common.folder.delete(tfo.Folder(name=args.folder_name))
    if args.update_connection:
        named_conn = datasource.connection.named_connections.get(args.conn_type)
        named_conn.connection.class_name = settings['embed_connection']['class_name']
        named_conn.connection.server = settings['embed_connection']['server']
        named_conn.connection.username = settings['embed_connection']['username']
        named_conn.connection.service = settings['embed_connection']['service']
        named_conn.connection.dbname = settings['embed_connection']['dbname']
        named_conn.connection.schema = settings['embed_connection']['schema']
        named_conn.connection.warehouse = settings['embed_connection']['warehouse']
        datasource.connection.named_connections[args.conn_type].connection = named_conn
    if tdsx:
        datasource.save()
    # if args.publish:
    #     server.publish_datasource(tdsx, dsid=args.id, name=args.name, project=args.project)
    if args.embed_creds:
        server.embed_datasource_credentials(
            args.id,
            credentials={
                'username': settings['embed_connection']['username'],
                'password': settings['embed_connection']['password']
            },
            connection_type=settings['embed_connection']['class_name']
        )
    if args.refresh:
        server.refresh_datasource(args.id)
        print(f'Refreshed {args.id}')


if __name__ == '__main__':
    main()
