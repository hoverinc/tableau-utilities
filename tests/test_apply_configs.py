import pytest
from typing import Dict, Any
from tableau_utilities.scripts.apply_configs import ApplyConfigs


@pytest.fixture
def apply_configs():
    return ApplyConfigs(datasource_name="my_datasource_1", datasource_path="", column_config={}, calculated_column_config={}, debugging_logs=False)



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

def test_invert_config_combined(apply_configs):
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

    expected_output = {
        "my_datasource_1": {
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

# def test_select_matching_datasource_config():
#     sample_config = {
#         "my_datasource_1": {
#             "Column2": {
#                 "description": "Description of Column2",
#                 "folder": "Folder2",
#                 "persona": "string_dimension",
#                 "local-name": "MY_COLUMN_1",
#                 "remote_name": "MY_COLUMN_1_ALIAS"
#             }
#         },
#         "my_datasource_2": {
#             "Column2": {
#                 "description": "Description of Column2",
#                 "folder": "Folder2",
#                 "persona": "string_dimension",
#                 "local-name": "MY_COLUMN_2",
#                 "remote_name": "MY_COLUMN_2_ALIAS"
#             }
#         }
#     }
#
#     expected_output = {
#         "my_datasource_1": {
#             "Column2": {
#                 "description": "Description of Column2",
#                 "folder": "Folder2",
#                 "persona": "string_dimension",
#                 "local-name": "MY_COLUMN_1",
#                 "remote_name": "MY_COLUMN_1_ALIAS"
#             }
#         },
#     }
#
#     result = apply_configs.select_matching_datasource_config(sample_config)
#     assert result == expected_output

if __name__ == '__main__':
    pytest.main()
