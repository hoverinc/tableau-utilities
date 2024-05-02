from tableau_utilities.scripts.merge_config import read_file
import pandas as pd


def csv_config(args):

    # Set variables from args
    debugging_logs = args.debugging_logs
    config_list = args.config_list

    all_columns = []

    for each_config in config_list:
        config = read_file(each_config)

        # columns = []
        for column, details in config.items():
            if debugging_logs:
                print(column, details)

            column_name = column
            description = details['description']
            folder = details['folder']
            persona = details['persona']

            calculation = details.get('calculation')
            fiscal_year_start = details.get('fiscal_year_start')
            default_format = details.get('default_format')

            for datasource in details['datasources']:
                datasource_name = datasource['name']
                local_name = datasource['local-name']
                sql_alias = datasource.get('sql_alias')

                each_column = {'column_name': column_name,
                               'description': description,
                               'folder': folder,
                               'persona': persona,
                               'calculation': calculation,
                               'fiscal_year_start': fiscal_year_start,
                               'default_format': default_format,
                               'datasource_name': datasource_name,
                               'local_name': local_name,
                               'sql_alias': sql_alias
                               }

                all_columns.append(each_column)

    df = pd.DataFrame(all_columns)
    df.to_csv('config_columns.csv', index=False)


