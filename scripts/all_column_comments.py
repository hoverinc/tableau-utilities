"""
    A script to gather column comments from various systems,
    including Snowflake, Postgres, and Tableau,
    and log them all to a CSV file.
"""

import os
import pandas as pd
import snowflake.connector
import xml.etree.ElementTree as ET

from sqlalchemy import create_engine
from tabulate import tabulate
from settings import snowflake_credentials, postgresql_credentials
from general import get_datasource_files_dict, get_datasource_fields_df


# ****************************************************************************************************
# SNOWFLAKE COMMENTS
def get_column_comments_snowflake():
    from snowflake.sqlalchemy import URL

    engine = create_engine(URL(**snowflake_credentials))
    connection = engine.connect()

    try:
        databases_df = pd.read_sql_query("show databases", engine)
        databases = databases_df.name.tolist()
        print('DATABASES!')
        print(databases)
        all_database_columns = pd.DataFrame()

        for database in databases:
            if database in ['SNOWFLAKE_SAMPLE_DATA', 'SFSUPPORT_DB']:
                continue

            all_columns_in_db = f'''
                SELECT
                    table_catalog AS database_name
                    , TABLE_SCHEMA AS schema_name
                    , TABLE_NAME
                    , COLUMN_NAME
                    , DATA_TYPE
                    , ORDINAL_POSITION
                    , IS_NULLABLE
                    , NULL AS is_updatable
                    , COLUMN_DEFAULT
                    , CHARACTER_MAXIMUM_LENGTH
                    , CHARACTER_OCTET_LENGTH
                    , NUMERIC_PRECISION
                    , NUMERIC_PRECISION_RADIX
                    , NUMERIC_SCALE
                    , DATETIME_PRECISION
                    , INTERVAL_TYPE
                    , INTERVAL_PRECISION
                    , COMMENT
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
            '''
            df = pd.read_sql_query(all_columns_in_db, engine)
            print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
            all_database_columns = all_database_columns.append(df, ignore_index=True)

        all_database_columns['data_source_type'] = 'SNOWFLAKE'

    finally:
        connection.close()
        engine.dispose()

    return all_database_columns


# ****************************************************************************************************
# POSTGRES COMMENTS
def get_column_comments_postgres(db_config):
    from sqlalchemy.engine.url import URL

    print(db_config)
    engine = create_engine(URL(drivername='postgres', **db_config))

    sql = '''
        SELECT
            cols.table_catalog as database_name
            , cols.table_schema as schema_name
            , cols.table_name
            , cols.column_name
            , cols.data_type
            , cols.ordinal_position
            , cols.is_nullable
            , cols.is_updatable
            , cols.column_default
            , cols.character_maximum_length
            , cols.character_octet_length
            , cols.numeric_precision
            , cols.numeric_precision_radix
            , cols.numeric_scale
            , cols.DATETIME_PRECISION
            , cols.INTERVAL_TYPE
            , cols.INTERVAL_PRECISION
            , (SELECT pg_catalog.col_description(c.oid, cols.ordinal_position::int)
                FROM pg_catalog.pg_class c
                WHERE c.oid = (SELECT cols.table_name::regclass::oid)
                  AND c.relname = cols.table_name) as comment
        FROM information_schema.columns cols
        WHERE cols.table_schema = 'public'
    '''
    df = pd.read_sql(sql, engine, params=None)
    return df


def all_database_column_comments_postgres():
    db_configs = [postgresql_credentials]

    all_datasources_df = pd.DataFrame()

    for db_config in db_configs:
        print(db_config)
        df = get_column_comments_postgres(db_config=db_config)
        print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
        all_datasources_df = all_datasources_df.append(df, ignore_index=True)

    all_datasources_df['data_source_type'] = 'POSTGRES'

    return all_datasources_df


# ****************************************************************************************************
# LOCAL TABLEAU COMMENTS
def get_data_definitions(all_files_dict):
    """
    Args:
        all_files_dict (dict): A dictionary with a key as the datasource name and a value of the datasource directory
    Returns: A dataframe with the data definitions from all of the tableau data sources
    """
    all_datasources_df = pd.DataFrame()
    for file, directory in all_files_dict.items():
        tree = ET.parse(os.path.join(directory, file)+'.twb')
        root = tree.getroot()
        df = get_datasource_fields_df(file, root)
        all_datasources_df = all_datasources_df.append(df, ignore_index=True)

    all_datasources_df['data_source_type'] = 'Tableau'
    all_datasources_df['database'] = None
    all_datasources_df['schema_name'] = None
    all_datasources_df['table_name'] = None
    all_datasources_df = all_datasources_df.rename(
        columns={
            'column': 'column_name',
            'data source': 'tableau_source',
            'definition': 'comment',
            'column_formulas': 'tableau_formulas',
            'column_datatype': 'data_type'
        }
    )

    return all_datasources_df[[
        'database',
        'schema_name',
        'table_name',
        'column_name',
        'data_type',
        'comment',
        'tableau_source',
        'tableau_formulas',
        'data_source_type'
    ]]


def tableau_definitions():
    all_files_dict = get_datasource_files_dict('datasources/')
    df = get_data_definitions(all_files_dict)
    return df


# ****************************************************************************************************
# MAIN SCRIPT
def main():
    # ['database', 'schema_name', 'table_name', 'column_name', 'data_type', 'comment']
    snowflake_df = get_column_comments_snowflake()
    # [database, schema_name, table_name, column_name, data_type, comment]
    postgres_df = all_database_column_comments_postgres()
    # ['database', 'schema_name', 'table_name', 'column_name', 'data_type', 'comment', 'tableau_source', 'tableau_formulas']
    tableau_df = tableau_definitions()
    all_df = snowflake_df.append(postgres_df, ignore_index=True)
    all_df['tableau_source'] = None
    all_df['tableau_formulas'] = None
    all_df = all_df.append(tableau_df, ignore_index=True)
    all_df.to_csv('all_data_definitions.csv', index=False)


if __name__ == '__main__':
    main()
