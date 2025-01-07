import pytest
from typing import Dict, Any
from tableau_utilities.scripts.apply_configs import ApplyConfigs


@pytest.fixture
def apply_configs():
    return ApplyConfigs(datasource_name="my_datasource_1", datasource_path="", target_column_config={},
                        target_calculated_column_config={}, debugging_logs=False)


def test_invert_config_single_datasource(apply_configs):
    sample_config = {
        "Column1": {
            "description": "Description of Column1",
            "folder": "Folder1",
            "persona": "string_dimension",
            "datasources": [
                {
                    "name": "my_datasource_1",
                    "local-name": "MY_COLUMN_1",
                    "sql_alias": "MY_COLUMN_1_ALIAS"
                }
            ]
        }
    }

    expected_output = {
        "my_datasource_1": {
            "Column1": {
                "description": "Description of Column1",
                "folder": "Folder1",
                "persona": "string_dimension",
                "local-name": "MY_COLUMN_1",
                "remote_name": "MY_COLUMN_1_ALIAS"
            }
        }
    }

    result = apply_configs.invert_config(sample_config)
    assert result == expected_output


def test_invert_config_multiple_datasources(apply_configs):
    sample_config = {
        "Column2": {
            "description": "Description of Column2",
            "folder": "Folder2",
            "persona": "string_dimension",
            "datasources": [
                {
                    "name": "my_datasource_1",
                    "local-name": "MY_COLUMN_1",
                    "sql_alias": "MY_COLUMN_1_ALIAS"
                },
                {
                    "name": "my_datasource_2",
                    "local-name": "MY_COLUMN_2",
                    "sql_alias": "MY_COLUMN_2_ALIAS"
                }
            ]
        }
    }

    expected_output = {
        "my_datasource_1": {
            "Column2": {
                "description": "Description of Column2",
                "folder": "Folder2",
                "persona": "string_dimension",
                "local-name": "MY_COLUMN_1",
                "remote_name": "MY_COLUMN_1_ALIAS"
            }
        },
        "my_datasource_2": {
            "Column2": {
                "description": "Description of Column2",
                "folder": "Folder2",
                "persona": "string_dimension",
                "local-name": "MY_COLUMN_2",
                "remote_name": "MY_COLUMN_2_ALIAS"
            }
        }
    }

    result = apply_configs.invert_config(sample_config)
    assert result == expected_output

def test_flatten_to_list_of_fields(apply_configs):

    sample_dict = {
        'My Caption 1': {
            'description': 'A perfect description',
            'folder': 'My Folder',
            'local-name': 'MY_FIELD_1',
            'persona': 'string_dimension',
            'remote_name': 'MY_FIELD_1'
        },
        'My Caption 2': {
            'description': 'Another perfect description',
            'folder': 'My Folder',
            'local-name': 'MY_FIELD_2',
            'persona': 'string_dimension',
            'remote_name': 'MY_FIELD_2'
        }
    }

    expected_output = [
        {
            'Caption': 'My Caption 1',
            'description': 'A perfect description',
            'folder': 'My Folder',
            'local-name': 'MY_FIELD_1',
            'persona': 'string_dimension',
            'remote_name': 'MY_FIELD_1'
        },
        {
            'Caption': 'My Caption 2',
            'description': 'Another perfect description',
            'folder': 'My Folder',
            'local-name': 'MY_FIELD_2',
            'persona': 'string_dimension',
            'remote_name': 'MY_FIELD_2'
        }
    ]

    result = apply_configs.flatten_to_list_of_fields(sample_dict)
    assert result == expected_output


def test_prepare_configs(apply_configs):
    sample_config_A = {
        "Column1": {
            "description": "Description of Column1",
            "folder": "Folder1",
            "persona": "string_dimension",
            "datasources": [
                {
                    "name": "my_datasource_1",
                    "local-name": "MY_COLUMN_1",
                    "sql_alias": "MY_COLUMN_1_ALIAS"
                }
            ]
        },
        "Column2": {
            "description": "Description of Column2",
            "folder": "Folder2",
            "persona": "string_dimension",
            "datasources": [
                {
                    "name": "my_datasource_1",
                    "local-name": "MY_COLUMN_1",
                    "sql_alias": "MY_COLUMN_1_ALIAS"
                },
                {
                    "name": "my_datasource_2",
                    "local-name": "MY_COLUMN_2",
                    "sql_alias": "MY_COLUMN_2_ALIAS"
                }
            ]
        }
    }

    sample_config_B = {
        "# ID": {
            "description": "Distinct Count of the ID",
            "calculation": "COUNTD([ID])",
            "folder": "My Data",
            "persona": "continuous_number_measure",
            "datasources": [
                {
                    "name": "my_datasource_1",
                    "local-name": "MY_COLUMN_1",
                    "sql_alias": "MY_COLUMN_1_ALIAS"
                },
                {
                    "name": "my_datasource_2",
                    "local-name": "MY_COLUMN_2",
                    "sql_alias": "MY_COLUMN_2_ALIAS"
                }
            ],
            "default_format": "n#,##0;-#,##0"
        }
    }

    expected_output = {
        "Column1": {
            "description": "Description of Column1",
            "folder": "Folder1",
            "persona": "string_dimension",
            "local-name": "MY_COLUMN_1",
            "remote_name": "MY_COLUMN_1_ALIAS"
        },
        "Column2": {
            "description": "Description of Column2",
            "folder": "Folder2",
            "persona": "string_dimension",
            "local-name": "MY_COLUMN_1",
            "remote_name": "MY_COLUMN_1_ALIAS"
        },
        "# ID": {
            "description": "Distinct Count of the ID",
            "calculation": "COUNTD([ID])",
            "default_format": "n#,##0;-#,##0",
            "folder": "My Data",
            "persona": "continuous_number_measure",
            "local-name": "MY_COLUMN_1",
            "remote_name": "MY_COLUMN_1_ALIAS"
        }
    }

    result = apply_configs.prepare_configs(sample_config_A, sample_config_B)
    assert result == expected_output

def test_flatten_to_list_of_fields(apply_configs):

    sample_dict = {
        'My Caption 1': {
            'description': 'A perfect description',
            'folder': 'My Folder',
            'local-name': 'MY_FIELD_1',
            'persona': 'string_dimension',
            'remote_name': 'MY_FIELD_1'
        },
        'My Caption 2': {
            'description': 'Another perfect description',
            'folder': 'My Folder',
            'local-name': 'MY_FIELD_2',
            'persona': 'string_dimension',
            'remote_name': 'MY_FIELD_2'
        }
    }

    expected_output = [
        {
            'caption': 'My Caption 1',
            'description': 'A perfect description',
            'folder': 'My Folder',
            'local-name': 'MY_FIELD_1',
            'persona': 'string_dimension',
            'remote_name': 'MY_FIELD_1'
        },
            {'caption': 'My Caption 2',
            'description': 'Another perfect description',
            'folder': 'My Folder',
            'local-name': 'MY_FIELD_2',
            'persona': 'string_dimension',
            'remote_name': 'MY_FIELD_2'
        }
    ]

    result = apply_configs.flatten_to_list_of_fields(sample_dict)
    assert result == expected_output

def test_select_matching_datasource_config(apply_configs):

    sample_config = {
        "my_datasource_1": {
            "Column1": {
                "description": "Description of Column1",
                "folder": "Folder1",
                "persona": "string_dimension",
                "local-name": "MY_COLUMN_1",
                "remote_name": "MY_COLUMN_1_ALIAS"
            }
        },
        "my_datasource_2": {
            "Column2": {
                "description": "Description of Column2",
                "folder": "Folder2",
                "persona": "string_dimension",
                "local-name": "MY_COLUMN_2",
                "remote_name": "MY_COLUMN_2_ALIAS"
            }
        }
    }

    expected_output = {
            "Column1": {
                "description": "Description of Column1",
                "folder": "Folder1",
                "persona": "string_dimension",
                "local-name": "MY_COLUMN_1",
                "remote_name": "MY_COLUMN_1_ALIAS"
            }
    }
    result = apply_configs.select_matching_datasource_config(sample_config)
    assert result == expected_output


if __name__ == '__main__':
    pytest.main()
