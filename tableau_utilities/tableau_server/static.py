""" Static functionality of the TableauServer and Core classes """
import requests
from tableau_utilities.general.funcs import flatten_dict


class TableauConnectionError(Exception):
    """ An Exception in the TableauServer connection """
    pass


def bytes_to_mb(b):
    """ Converts Bytes to Megabytes """
    return b / 1024 / 1024


def mb_to_bytes(mb):
    """ Converts Megabytes to Bytes """
    return mb * 1024 * 1024


def transform_tableau_object(object_dict):
    """ Transforms the object dict from a Tableau REST API call
    Args:
        object_dict (dict): The object dict from a Tableau REST API call
    """
    if object_dict.get('tags'):
        object_dict['tags'] = [t['label'] for t in object_dict['tags']['tag']]
    update = dict()
    flatten_dict(object_dict, update)
    object_dict.clear()
    object_dict.update(update)


def validate_response(response):
    """ Validates the response received from an API call

    Args:
        response (requests.Response): A requests Response object
        raise_for_status (bool): True to raise an error on a bad response

    Returns: The response content as a JSON dict
    """
    info = response.json()
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        error = info.get('error', {})
        raise TableauConnectionError(
            f'\nError: {error.get("code")}: {error.get("summary")} - {error.get("detail")}\n{err}'
        ) from err
    return info
