from argparse import RawTextHelpFormatter
import argparse
import json
import os
import shutil
import sys
from pprint import pprint

from tableau_utilities.scripts.datasources_column_config_generate import main
from tableau_utilities.scripts.gen_config import generate_config


# def generate_config(args):
#     print("in the function")
#     print(args.datasource)

def do_args():
    """ Parse arguments.

    Returns: an argparse.Namespace
    """

    parser = argparse.ArgumentParser(description='Tableau Utilities CLI:\n'
                                                 '-Manage Tableau Server/Online\n'
                                                 '-Manage configurations to edit datasource metadata',
                                     formatter_class=RawTextHelpFormatter)
    parser = argparse.ArgumentParser(prog='PROG')
    # parser.add_argument('--foo', action='store_true', help='foo help')
    # parser.add_argument(prog='scriptname',  choices=['server_info', 'generate_config', 'merge_config'])
    subparsers = parser.add_subparsers(help='Select a script type to run',  required=True)


    parser.add_argument(
        '--server',
        help='Tableau Server URL. i.e. <server_address> in https://<server_address>.online.tableau.com',
        default=None
    )
    parser.add_argument(
        '--site',
        help='Site name. i.e. <site> in https://<server_address>.online.tableau.com/#/site/<site>',
        default=None)
    parser.add_argument('--api_version', help='Tableau API version', default='3.17')
    parser.add_argument('--user', help='user name')
    parser.add_argument('--password', help='password')
    parser.add_argument('--token_secret', help='Personal Access Token Secret')
    parser.add_argument('--token_name', help='Personal Access Token Name')

    # create the parser for the "a" command
    parser_config_gen = subparsers.add_parser('generate_config', help='a help')
    parser_config_gen.add_argument('--datasource', help='The name of the datasources to generate a config for')
    parser_config_gen.add_argument('--clean_up_first', action='store_true', help='Deletes the directory and files before running')
    parser_config_gen.add_argument('--folder_name', default='tmp_tdsx_and_config',
                        help='Specifies the folder to write the datasource and configs to')
    parser_config_gen.add_argument('--file_prefix', action='store_true',
                        help='Adds a prefix of the datasource name to the output file names')
    parser_config_gen.add_argument('--definitions_csv',
                        help='Allows a csv with definitions to be inputted for adding definitions to a config. It may be easier to populate definitions in a spreadsheet than in the configo ')
    parser_config_gen.set_defaults(func=generate_config)

    # create the parser for the "b" command
    parser_config_merge = subparsers.add_parser('merge_config', help='b help')
    parser_config_merge.add_argument('-e', '--existing_config',
                        help='The path to the current configuration')
    parser_config_merge.add_argument('-a', '--additional_config',
                        help='The path to the configuration. This code ASSUMES that the additional config is for a single datasource ')
    parser_config_merge.add_argument('-n', '--merged_config', default='merged_config',
                        help='The name of the merged config JSON file.  For my_config.json enter my_config. Do not enter the .json extension')
    parser_config_merge.add_argument('-f', '--folder_name', default='tmp_tdsx_and_config',
                        help='Specifies the folder to write the datasource and configs to')
    # parser_foo.set_defaults(func=foo)

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
    args.func(args)
    # print("i made it to this version")
    # print(args.scriptname)


if __name__ == '__main__':
    main()




    # if args.config_generate:
    #     datasources_column_config_generate.main()




