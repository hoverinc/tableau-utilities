import xml.etree.ElementTree as ET
import os
import pandas as pd
import pygsheets

from general import get_datasource_files_dict, get_datasource_fields_df


def clear_sheets_and_rename(sheet_obj, num_sheets):
    """ Goes through the number of sheets from the parameter
        and clears them all and resets the sheet title to be the sheet numbers

    Args:
        sheet_obj (obj): The google sheets object
        num_sheets (int): The number of sheets that you want to clear from the google sheet
    """
    # clear all sheets and rename
    for i in range(num_sheets):
        wks = sheet_obj[i]
        # rename the sheet
        wks.title = str(i)
        # clear the worksheet
        wks.clear()


def upload_data_definitions(sheet_obj, all_files_dict):
    """ Iterates through every file in the all_files_dict and gets then parses them using element tree
        and calls the get_datasource_fields_df function to get a dataframe of their fields.
        Finally, it takes the definitions from all of the datasources and adds them to the Google Sheet.

    Args:
        sheet_obj (obj): The google sheets object
        all_files_dict (dict): A dictionary with a key for the file name and value of the folder it's in
    """
    all_datasources_df = pd.DataFrame()
    wks = sheet_obj[0]
    wks.title = 'Tableau Datasource Definitions'
    # for k, v in all_files_dict.iteritems():
    for file, directory in all_files_dict.items():
        tree = ET.parse(os.path.join(directory, f'{file}.twb'))
        root = tree.getroot()
        df = get_datasource_fields_df(file, root)
        all_datasources_df = all_datasources_df.append(df, ignore_index=True)
    # Update the first sheet with all_datasources_df, starting at cell A1.
    wks.set_dataframe(all_datasources_df, (1, 1))


def main():
    """
        Calls all_files_dict to get all of the files, then it calls
        clear_sheets_and_rename to clear out the Datasource Definitions google sheet.
        Finally, it calls upload_data_definitions to upload the data definitions
        from every data source to the google sheet
    """
    # Authorizing google sheets
    gs = pygsheets.authorize()
    # Load worksheet by name
    sh = gs.open('Datasource Definitions')
    all_files_dict = get_datasource_files_dict('datasources/')
    clear_sheets_and_rename(sh, 1)
    upload_data_definitions(sh, all_files_dict)


if __name__ == "__main__":
    # calls the main function
    main()
