from argparse import RawTextHelpFormatter
import argparse
from tableau_utilities import TableauServer


parser = argparse.ArgumentParser(prog='list_datasources',
                                 description='List Datasources')

# GROUP: USER & PASSWORD
group_user_password = parser.add_argument_group('user_pass', 'Authentication with username and password method')
group_user_password.add_argument('--user', help='user name')
group_user_password.add_argument('--password', help='password')

# GROUP: PERSONAL ACCESS TOKENS
group_token = parser.add_argument_group('token_info', 'Authentication with a Personal Access Token (PAT)')
group_token.add_argument('--token_secret', help='Personal Access Token Secret')
group_token.add_argument('--token_name', help='Personal Access Token Name')

# GROUP: SERVER INFORMATION
group_server = parser.add_argument_group('server_information', 'Server Information')
group_server.add_argument(
    '--server',
    help='Tableau Server URL. i.e. <server_address> in https://<server_address>.online.tableau.com',
    default=None
)
group_server.add_argument(
    '--site',
    help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
    default=None)
group_server.add_argument('--api_version', help='Tableau API version', default='3.17')


def main():
    args = parser.parse_args()
    url = f'https://{args.server}.online.tableau.com'
    ts = TableauServer(
        user=args.user,
        password=args.password,
        personal_access_token_name=args.token_name,
        personal_access_token_secret=args.token_secret,
        host=url,
        site=args.site
    )
    ts.get.datasources(print_info=True)


if __name__ == '__main__':
    main()
