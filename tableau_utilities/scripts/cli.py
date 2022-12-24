from argparse import RawTextHelpFormatter
import argparse
import json
import os
import shutil
import sys
from pprint import pprint

from tableau_utilities.scripts.datasources_column_config_generate import main


def do_args():
    """ Parse arguments.

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(description='Tableau Utilities CLI:\n'
                                                 '-Manage Tableau Server/Online\n'
                                                 '-Manage configurations to edit datasource metadata',
                                     formatter_class=RawTextHelpFormatter)
    parser.add_argument('scriptname',  choices=['server_info', 'generate_config', 'merge_config'])

    # parser.add_argument('-g', '--config_generate', action='store_true',
    #                     help='CLI to generate column configs to automated editing of Tableau medatadata')
    # parser.add_argument('-m', '--conffig_merge',
    # parser.add_argument('-s', '--server_info',
    #                     help='The name of the merged config JSON file.  For my_config.json enter my_config. Do not enter the .json extension')
    # parser.add_argument('-m', '--folder_name',
    #                     help='Specifies the folder to write the datasource and configs to')

    return parser.parse_args()


def main():
    args = do_args()
    print("i made it to this version")
    print(args.scriptname)


if __name__ == '__main__':
    main()




    # if args.config_generate:
    #     datasources_column_config_generate.main()




