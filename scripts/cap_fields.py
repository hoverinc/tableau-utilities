import xml.etree.ElementTree as ET
import os
import re

from general import convert_underscore_to_spaces_and_capitalize, get_datasource_files_dict


# function to get all of the definitions from a datasource
def rename_fields(tree, root):
    """
    Args:
        tree (obj): the tree from parsing the xml using element tree
        root (obj): the root from parsing the xml using element tree
    Returns: the updated tableau tree and root that have the new field names
    """

    # going through every column in the tableau workbook
    for column in root.iter('column'):
        column_name = column.attrib.get('caption')
        field_name = column.attrib.get('name')
        field_name = field_name.replace(' ', '_')

        if column_name is not None and 'do_not_rename' not in column_name:
            # print 'Old Column Name: ' + column.attrib['caption']
            # column.attrib['caption'] = convert_underscore_to_spaces_and_capitalize(column_name)
            # print 'New Column Name: ' + column.attrib['caption']
            print(column_name)
            if '[Calculation_' not in field_name:
                column.attrib['name'] = field_name.upper()
            print(column.attrib['name'])

    for calculation in root.iter('calculation'):
        formula = calculation.attrib.get('formula')
        if formula is not None:
            regex = '\[.*?\]'
            for m in re.finditer(regex, formula):

                if '[Calculation' not in m.group(0):
                    new_formula = formula.replace(m.group(0), m.group(0).upper())
                    new_formula = new_formula.replace(' ', '_')
                    calculation.attrib['formula'] = new_formula

    for folder in root.iter('folder-item'):
        field_name = folder.attrib.get('name')
        if field_name is not None and '[Calculation' not in field_name:
            folder.attrib['name'] = field_name.upper().replace(' ', '_')

    # return the updated tableau tree and root
    return tree, root


def all_files(all_files_dict):
    """ Goes through the all of the fields in every shared datasource and renames them to not have underscores
        and to capitalize the first letter of each word

    Args:
        all_files_dict (dict): A dictionary with a key for the file name and value of the folder it's in
    """
    # go through all the files
    for k, v in all_files_dict.items():
        # file is the key from the dictionary
        file = k
        # set the path to the file
        path = os.path.join(v, f'{file}.twb')
        # set the tree and root
        tree = ET.parse(path)
        root = tree.getroot()
        # update the tree and root by calling the write definitions function to add definitions
        tree, root = rename_fields(tree, root)
        # save the file
        tree.write(path)


def main():
    """
        Gathers all of the shared datasources and renames every field in there
        if they have underscores (and do not contain the text 'do_not_rename')
    """
    # gathering all of the shared datasources
    all_files_dict = get_datasource_files_dict('datasources/')
    # all_files_dict = {'datasource_name': 'path/to/datasource'}
    print(all_files_dict)
    all_files(all_files_dict=all_files_dict)


if __name__ == "__main__":
    main()
