import os
import pandas as pd


def convert_underscore_to_spaces_and_capitalize(name):
    """

    Args:
        name (str): A string that you want to rename

    Returns: the string with underscores converted to spaces and the first letter of the word capitalized
    """
    return ' '.join(x.title() for x in name.split('_'))


def get_datasource_files_dict(datasources_directory):
    """ Gathers all datasources and adding to dictionary

    Args:
        datasources_directory (str): The path to local datasource files (with twb extension)

    Returns: A dictionary with a key as the datasource name and a value of the datasource directory
    """
    all_files_dict = {}
    for directory, folders, files in os.walk(datasources_directory):
        for name in files:
            if name[0] != '.' and name.split('.')[-1] == 'twb':
                all_files_dict[name.split('.')[0]] = directory

    return all_files_dict


def get_datasource_fields_df(datasource, root):
    """ Lists all columns from a datasource in a dataframe.

    Args:
        datasource (str): The name of the datasource that you want to get all of the definitions from.
        root (obj): the root from parsing the xml using element tree

    Returns: A dataframe that has a row for every field in the workbook. it lists the field's name, caption, definition, and formula
    """
    column_names = []
    column_definitions = []
    column_formulas = []
    column_exact_names = []
    column_datatype = []
    for column in root.iter('column'):
        definition = ''
        formula = ''

        try:
            column_names.append(column.attrib.get('caption'))
            column_exact_names.append(column.attrib.get('name'))
            column_datatype.append(column.attrib.get('datatype'))
        except TypeError:
            column_names.append('not a real column')
            column_exact_names.append('not a real column')
            column_datatype.append(column.attrib.get('not a real column'))

        for desc in column.iter('run'):
            definition = desc.text
        if definition != '':
            column_definitions.append(definition)
        else:
            column_definitions.append('missing definition')

        for formula in column.iter('calculation'):
            formula = formula.attrib.get('formula')
        if formula != '':
            column_formulas.append(formula)
        else:
            column_formulas.append('')

    df = pd.DataFrame({
        'column': column_names,
        'tableau_name': column_exact_names,
        'definition': column_definitions,
        'column_formulas': column_formulas,
        'column_datatype': column_datatype
    })
    df = df.loc[(df.column != 'not a real column') & (df.column != '') & (df.column.notna())]
    df['data source'] = datasource
    return df