# test_remove_empty_folders.py
import pytest
from unittest.mock import patch
from tableau_utilities.tableau_file.tableau_file_objects import FoldersCommon, Folder, FolderItem
from tableau_utilities.tableau_file.tableau_file import Datasource

@pytest.fixture
def mock_datasource():
    with patch('tableau_utilities.tableau_file.tableau_file.Datasource.__init__', lambda x, file_path: None):
        datasource = Datasource(file_path='dummy_path')

        # Create the mock data
        mock_folders = [
            Folder(
                name='Folder - 2 columns',
                tag='folder',
                role=None,
                folder_item=[
                    FolderItem(name='[COLUMN_1]', type='field', tag='folder-item'),
                    FolderItem(name='[COLUMN_2]', type='field', tag='folder-item')
                ]
            ),
            Folder(
                name='Folder - Empty',
                tag='folder',
                role=None,
                folder_item=[]
            ),
            Folder(
                name='People',
                tag='folder',
                role=None,
                folder_item=[
                    FolderItem(name='[COLUMN_2+3]', type='field', tag='folder-item')
                ]
            )
        ]

        # Assign the mock folders to the folders_common attribute
        folders_common = FoldersCommon(folder=mock_folders)
        datasource.folders_common = folders_common

        return datasource

def test_remove_empty_folders_removed_folders(mock_datasource):
    removed_folders = mock_datasource.remove_empty_folders()
    assert removed_folders == ['Folder - Empty']

def test_remove_empty_folders_folder_count(mock_datasource):
    mock_datasource.remove_empty_folders()
    assert len(mock_datasource.folders_common.folder) == 2

def test_remove_empty_folders_folder_names(mock_datasource):
    mock_datasource.remove_empty_folders()
    folder_names = [folder.name for folder in mock_datasource.folders_common.folder]
    assert 'Folder - Empty' not in folder_names

if __name__ == '__main__':
    pytest.main()
