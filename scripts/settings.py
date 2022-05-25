from os import environ

VERSION = '2.7'

snowflake_credentials = {
    'user': environ.get('SNOWFLAKE_USERNAME'),
    'password': environ.get('SNOWFLAKE_PASSWORD'),
    'account': environ.get('SNOWFLAKE_ACCOUNT'),
    'database': environ.get('SNOWFLAKE_DATABASE'),
    'schema': environ.get('SNOWFLAKE_SCHEMA'),
    'warehouse': environ.get('SNOWFLAKE_WAREHOUSE'),
    'role': environ.get('SNOWFLAKE_ROLE')
}

tableau_credentials = {
    'server': environ.get('TABLEAU_ONLINE_URL'),
    'site_id': environ.get('TABLEAU_ONLINE_SITE_ID'),
    'username':  environ.get('TABLEAU_ONLINE_USERNAME'),
    'password': environ.get('TABLEAU_ONLINE_PASSWORD')
}

postgresql_credentials = {
    'host': environ.get('POSTGRESQL_HOST'),
    'port': environ.get('POSTGRESQL_PORT'),
    'username': environ.get('POSTGRESQL_USERNAME'),
    'password': environ.get('POSTGRESQL_PASSWORD'),
    'database': environ.get('POSTGRESQL_DATABASE')
}
