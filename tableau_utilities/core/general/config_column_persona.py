personas = {
    "string_dimension": {
        "role": "dimension",
        "role_type": "nominal",
        "datatype": "string"
    },
    "date_dimension": {
        "role": "dimension",
        "role_type": "ordinal",
        "datatype": "date"
    },
    "datetime_dimension": {
        "role": "dimension",
        "role_type": "ordinal",
        "datatype": "datetime"
    },
    "date_measure": {
        "role": "measure",
        "role_type": "ordinal",
        "datatype": "date"
    },
    "datetime_measure": {
        "role": "measure",
        "role_type": "ordinal",
        "datatype": "datetime"
    },
    "discrete_number_dimension": {
        "role": "dimension",
        "role_type": "ordinal",
        "datatype": "integer"
    },
    "continuous_number_dimension": {
        "role": "dimension",
        "role_type": "quantitative",
        "datatype": "integer"
    },
    "discrete_number_measure": {
        "role": "measure",
        "role_type": "ordinal",
        "datatype": "integer"
    },
    "continuous_number_measure": {
        "role": "measure",
        "role_type": "quantitative",
        "datatype": "integer"
    },
    "discrete_decimal_dimension": {
        "role": "dimension",
        "role_type": "ordinal",
        "datatype": "real"
    },
    "continuous_decimal_dimension": {
        "role": "dimension",
        "role_type": "quantitative",
        "datatype": "real"
    },
    "discrete_decimal_measure": {
        "role": "measure",
        "role_type": "ordinal",
        "datatype": "real"
    },
    "continuous_decimal_measure": {
        "role": "measure",
        "role_type": "quantitative",
        "datatype": "real"
    },
    "boolean_dimension": {
        "role": "dimension",
        "role_type": "nominal",
        "datatype": "boolean"
    },
    "boolean_measure": {
        "role": "measure",
        "role_type": "nominal",
        "datatype": "boolean"
    }
}


def get_persona_by_attribs(role, role_type, datatype):
    """ Gets the name of the Persona based on the attributes provided.

    Args:
        role (str): The role of the persona
        role_type (str): The type of the persona
        datatype (str): The datatype of the persona

    Returns: The name of the persona
    """
    search_dict = {'role': role, 'role_type': role_type, 'datatype': datatype}
    for name, attribs in personas.items():
        if attribs == search_dict:
            return name
    return None


def get_persona_by_metadata_local_type(local_type):
    """ Gets the name of the persona based on the local_type of a metadata record.

    Args:
        local_type: The local_type attribute of a metadata record

    Returns: The name of the persona
    """
    # I think it's low risk to assume these data types are all dimensions
    if local_type == 'string':
        return 'string_dimension'
    elif local_type == 'date':
        return 'date_dimension'
    elif local_type == 'datetime':
        return 'datetime_dimension'
    elif local_type == 'boolean':
        return 'boolean_dimension'
    # Makes the assumption that numbers are discrete measures so that a user can't do accidental math on fields
    elif local_type == 'integer':
        return 'discrete_number_dimension'
    elif local_type == 'real':
        return 'discrete_decimal_dimension'

    return None


if __name__ == '__main__':
    print(get_persona_by_attribs('measure', 'nominal', 'boolean'))
    print(get_persona_by_metadata_local_type('string'))
