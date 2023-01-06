"""
Generic Python functions
"""
import re
from copy import deepcopy


def convert_to_snake_case(string):
    """ Converts a string to snake_case
    Args:
        string (str): A string to convert
    Returns: The transformed string
    """
    # Convert '_This...Thing+123 here__' to 'This_Thing_123_here'
    string = re.sub(r'[^a-zA-Z\d]+', ' ', string)
    string = string.strip().replace(' ', '_')
    # Convert 'ThisThing' to 'this_thing'
    string = re.sub(r'([a-z])([A-Z])', r'\1_\2', string).lower()
    return string


def flatten_dict(dictionary, final_dict, prior_key=None):
    """ Flattens a dictionary.
        Updates the keys to a snake_case in the path of keys flattened.
    Sample:
        - Input:
         {'first_level1': {'second_level1': 123, 'second_level2': 456}, 'first_level2': 'value'}
        - Output:
         {'first_level1_second_level1': 123, 'first_level1_second_level2': 456, 'first_level2': 'value'}
    Args:
        dictionary (dict): The dictionary to flatten
        final_dict (dict): A dictionary to add the flattened values to
        prior_key (str): Should not be given when first calling flatten_dict
    Returns: The flattened dictionary
    """
    for k, v in dictionary.items():
        # Combine the prior_key, with the key in the loop
        current_key = f'{prior_key}_{k}' if prior_key else k
        current_key = convert_to_snake_case(current_key)
        if isinstance(v, dict):
            flatten_dict(v, final_dict, current_key)
        else:
            if current_key not in final_dict:
                final_dict[current_key] = v
            else:
                raise KeyError(f'Attempted to flatten: {current_key}, already in {dictionary}')


def transform_tableau_object(obj):
    """ Transform a Tableau item.

    Args:
        obj (dict): The dict Tableau object
    """
    # Transform the item dict
    update = dict()
    for key, value in obj.items():
        # Update item keys "@someThing-like...this" to "this"
        key = re.sub(r'.+\.\.\.', '', key)
        key = key.replace('ns0_', '').replace('_ns0', '')
        key = convert_to_snake_case(key)
        if key == 'class':
            update['class_name'] = value
        else:
            update[key] = deepcopy(value)
    return update
