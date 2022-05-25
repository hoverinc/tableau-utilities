import xml.etree.ElementTree as ET
import os
import pandas as pd
import pygsheets
from general import get_datasource_files_dict


# function to get all of the definitions from a datasource
def write_definitions(tree, root, new_definitions):
    """
    Args:
        tree: the tree from parsing the xml using element tree
        root: the root from parsing the xml using element tree
        new_definitions: a dataframe that contains the workbook name, field name, and new data definitions

    Returns: The updated tableau tree and root that have the new data definitions
    """
    # list of all of the fields in the google sheet
    fields_new_definitions = list(new_definitions['Field'])
    # list of all of the definitions in the google sheet
    values_new_definitions = list(new_definitions['Definition'])
    # dictionary of the fields, definitions from the google sheet
    new_definitions_dict = dict(zip(fields_new_definitions, values_new_definitions))

    # going through every column in the tableau workbook
    for column in root.iter('column'):
        has_definition = False
        # getting the column name from the caption
        column_name = column.attrib.get('caption')
        # if the column name is in the google sheet that contains fields that need a new definition
        if column_name in fields_new_definitions:
            # new definition of the field is the definition from the google sheet
            new_definition = new_definitions_dict[column_name]
            # if the tableau field already has a "run" element (means it already has a definition)
            for desc in column.iter('run'):
                has_definition = True
                # update the definition to be the new_definition
                desc.text = new_definition
            # if the tableau field didn't have a run element (so it didn't have a definition)
            if not has_definition:
                has_definition = True
                # create a desc element
                desc = ET.SubElement(column, 'desc')
                # create a formatted-text subelement of desc
                formatted_text = ET.SubElement(desc, 'formatted-text')
                # create a run subelement of formatted_text
                run = ET.SubElement(formatted_text, 'run')
                # set the definition to the definition from google sheets
                run.text = new_definition

    # return the updated tableau tree and root
    return tree, root


def read_google_sheets(sheet_name):
    """
    Args:
        sheet_name (str): The name of the google sheet

    Returns: A dataframe that is all the data from the google sheet called New Definitions
    """
    # open the first tab in the google sheet
    gc = pygsheets.authorize()
    sh = gc.open(sheet_name)
    wks = sh.sheet1
    # Get worksheet values
    google_sheet = wks.get_all_values()
    headers = google_sheet.pop(0)
    # Convert worksheet to dataframe
    df = pd.DataFrame(google_sheet, columns=headers)
    df = df.drop([''], axis=1)
    # return the dataframe
    return df


def all_files(all_files_dict, new_definitions):
    """ Goes through the new definitions dataframe and updates the data definitions
        of the fields in the appropriate shared datasources workbook based on the new definitions dataframe.

    Args:
        all_files_dict: a dictionary with a key for the file name and value of the folder it's in
        new_definitions: a dataframe that contains the workbook name, field name, and new data definitions
    """
    # go through all the files
    for k, v in all_files_dict.iteritems():
        # file is the key from the dictionary
        file = k
        # set the path to the file
        path = os.path.join(os.environ.get("TABLEAU_PATH"),'shared_datasources', v, file)+'.twb'
        # subset of google sheet where the workbook name is the file you are in
        datasource_new_definitions = new_definitions[new_definitions['Workbook Name']==file]
        # if there are any records in the google sheet for the current file
        if len(datasource_new_definitions) > 0:
            # set the tree and root
            tree = ET.parse(path)
            root = tree.getroot()
            # update the tree and root by calling the write definitions function to add definitions
            tree, root = write_definitions(tree, root, datasource_new_definitions)
            # save the file
            tree.write(path)


def main():
    """
        Gathers all of the shared datasources, goes through the google sheet that has the new definitions,
        and applies to the new definitions to the fields in the datasources.
    """
    # gathering all of the shared datasources
    all_files_dict = get_datasource_files_dict('datasources/')
    # going through the google sheet that has the new definitions
    new_definitions = read_google_sheets('New Definitions')
    # applying new definitions to the fields in the shared datasources
    all_files(all_files_dict=all_files_dict, new_definitions=new_definitions)


# run the script
if __name__ == "__main__":
    # calls the main function
    main()
