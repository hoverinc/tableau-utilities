import argparse
import json
import os
import shutil
from pprint import pprint

from tableau_utilities.tableau_file.tableau_file import Datasource
from tableau_utilities.tableau_server.tableau_server import TableauServer


def do_args():
    """ Parse arguments.

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(description='List columns in Tableau datasources')
    parser.add_argument('-e', '--existing_config',
                        help='The path to the current configuration')
    parser.add_argument('-a', '--additional_config',
                        help='The path to the configuration. This config ')

    return parser.parse_args()


def merge_configs(existing_config, additional_config):

    print(existing_config)
    print(additional_config)

    # for column in existing_config:
    #     print(column)
    #
    # for column in additional_config:
    #     print(column)

if __name__ == '__main__':
    args = do_args()

    merge_configs(args.existing_config, args.additional_config)
