import pytest
import tableau_utilities
import collections
import shutil
import os
from pathlib import Path


def make_argv(user='fred', password='toast'):
    argv = [
        '--user', user,
        '--password', password,
        '--project', 'test_tableau_utilities',
        '--name', 'feelies',
        '--tdsx', 'test_data_source.tdsx',
    ]
    return argv


def add_column_args():
    argv = make_argv()
    args = tableau_utilities.do_args(argv)
    args.caption = 'Friendly Name'
    args.datatype = 'string'
    args.column_name = 'FRIENDLY_NAME'
    args.role = 'dimension'
    args.desc = 'Nice and friendly'
    return args


def add_folder_args():
    argv = make_argv()
    args = tableau_utilities.do_args(argv)
    args.folder_name = 'Friendly Name'
    args.role = 'dimension'
    return args


# TEST 1: do_args()
def test_do_args():
    argv = make_argv()
    args = tableau_utilities.do_args(argv)
    assert args.name == 'feelies'


# TEST 2: extract_tds()
def test_extract_tds():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        tds = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        assert isinstance(tds, type(collections.OrderedDict()))


# TEST 3: update_tds()
def test_update_tds():
    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        tds_before = tableau_utilities.extract_tds('test_data_source.tdsx')
        junk_tds_file = open('a_junk_tds.tds', 'w').close()
        tableau_utilities.update_tdsx('test_data_source.tdsx', tds_before)
        tds_after = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        remaing_tds_file = list(Path('.').glob('*.tds'))[0]
        os.remove(remaing_tds_file)
        assert tds_after == tds_before
        assert str(remaing_tds_file) == 'a_junk_tds.tds'


# TEST 4: add_column()
def test_add_column():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        args = add_column_args()
        column_name = 'COOL_NAME'
        caption = 'Cool Name'
        role = 'dimension'
        folder = 'tidy'
        tds_path = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        attribs = {
            'item_type': 'column',
            'column_name': column_name,
            'caption': caption,
            'folder_name': folder,
            'role': role
        }
        tds = tableau_utilities.TDS(tds_path)
        tds.add(**attribs)
        col = tds.get(**attribs)
        folder = tds.get('folder', folder_name=folder, role=role)
        folder_item_added = False
        if isinstance(folder['folder-item'], list):
            folder_items = folder['folder-item']
        else:
            folder_items = [folder['folder-item']]
        for f in folder_items:
            if f and f['@name'] == f'[{column_name}]':
                folder_item_added = True
        assert col is not None
        assert folder_item_added


# TEST 5: add_column()
def test_add_existing_column_fails():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        args = add_column_args()
        add_existing_column_fails = False
        tds_path = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        attribs = {
            'item_type': 'column',
            'column_name': args.column_name,
            'caption': args.caption,
            'folder_name': 'tidy',
            'role': args.role,
            'role_type': args.role_type,
            'datatype': args.datatype,
            'desc': args.desc
        }
        tds = tableau_utilities.TDS(tds_path)
        tds.add(**attribs)
        try:
            tds.add(**attribs)
        except tableau_utilities.tableau_utilities.TableauUtilitiesError:
            add_existing_column_fails = True
        assert add_existing_column_fails


# TEST 6: add_column()
def test_add_column_fails_with_wrong_folder():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        args = add_column_args()
        tds_path = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        got_a_TableauUtilitiesError = False
        try:
            tableau_utilities.TDS(tds_path).add(
                item_type='column',
                column_name=args.column_name,
                caption=args.caption,
                folder_name='not_here',
                role=args.role,
                role_type=args.role_type,
                datatype=args.datatype,
                desc=args.desc,
                calculation=args.calculation
            )
        except tableau_utilities.tableau_utilities.TableauUtilitiesError:
            got_a_TableauUtilitiesError = True
        assert got_a_TableauUtilitiesError


# TEST 7: add_folder()
def test_add_folder_tdsx_has_folder():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        args = add_folder_args()
        tds_path = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        attribs = {
            'item_type': 'folder',
            'folder_name': args.folder_name,
            'role': args.role
        }
        tds = tableau_utilities.TDS(tds_path)
        tds.add(**attribs)
        found_folder = tds.get(**attribs)
        assert found_folder is not None


# TEST 8: add_folder()
def test_add_folder_tdsx_does_not_have_folder():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/no_folder.tdsx', 'no_folder.tdsx')
        args = add_folder_args()
        tds_path = tableau_utilities.extract_tds('no_folder.tdsx')
        os.remove('no_folder.tdsx')
        attribs = {
            'item_type': 'folder',
            'folder_name': args.folder_name,
            'role': args.role
        }
        tds = tableau_utilities.TDS(tds_path)
        tds.add(**attribs)
        found_folder = tds.get(**attribs)
        assert found_folder is not None


# TEST 9: add_folder()
def test_add_folder_tdsx_has_one_folder():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/one_folder.tdsx', 'one_folder.tdsx')
        args = add_folder_args()
        tds_path = tableau_utilities.extract_tds('one_folder.tdsx')
        os.remove('one_folder.tdsx')
        attribs = {
            'item_type': 'folder',
            'folder_name': args.folder_name,
            'role': args.role
        }
        tds = tableau_utilities.TDS(tds_path)
        tds.add(**attribs)
        found_folder = tds.get(**attribs)
        assert found_folder is not None


# TEST 10: add_folder()
def test_add_existing_folder_fails():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        args = add_folder_args()
        tds_path = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        add_existing_folder_fails = False
        attribs = {
            'item_type': 'folder',
            'folder_name': args.folder_name,
            'role': args.role
        }
        tds = tableau_utilities.TDS(tds_path)
        tds.add(**attribs)
        try:
            tds.add(**attribs)
        except tableau_utilities.tableau_utilities.TableauUtilitiesError:
            add_existing_folder_fails = True
        assert add_existing_folder_fails


# TEST 11: delete_folder()
def test_delete_folder():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        args = add_folder_args()
        tds_path = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        attribs = {
            'item_type': 'folder',
            'folder_name': args.folder_name,
            'role': args.role
        }
        tds = tableau_utilities.TDS(tds_path)
        tds.add(**attribs)
        tds.delete(**attribs)
        found_folder = tds.get(**attribs)
        assert not found_folder


# TEST 12: modify_column()
def test_modify_column():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        tds_path = tableau_utilities.extract_tds('test_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        attribs = {
            'item_type': 'column',
            'column_name': 'QUANTITY',
            'caption': 'Quantity'
        }
        tds = tableau_utilities.TDS(tds_path)
        tds.update(**attribs)
        col = tds.get(**attribs)
        assert col['@caption'] == 'Quantity'


# TEST 13: find_folder()
def test_get_folder():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        shutil.copyfile(f'resources/{folder}/test_live_data_source.tdsx', 'test_live_data_source.tdsx')
        shutil.copyfile(f'resources/{folder}/one_folder.tdsx', 'one_folder.tdsx')
        tds_extract = tableau_utilities.extract_tds('test_data_source.tdsx')
        tds_live = tableau_utilities.extract_tds('test_live_data_source.tdsx')
        tds_one_folder = tableau_utilities.extract_tds('one_folder.tdsx')
        os.remove('test_data_source.tdsx')
        os.remove('test_live_data_source.tdsx')
        os.remove('one_folder.tdsx')
        assert tableau_utilities.TDS(tds_extract).get(
            item_type='folder', folder_name='tidy', role='dimension') is not None
        assert tableau_utilities.TDS(tds_live).get(
            item_type='folder', folder_name='tidy', role='dimension') is None
        assert tableau_utilities.TDS(tds_one_folder).get(
            item_type='folder', folder_name='neat', role='measure') is not None


# TEST 14: find_all_folders()
def test_find_all_folders():

    def compare_list(outcome, expected):
        # Compare each item because sorting affects comparison
        if not isinstance(outcome, list):
            return outcome == expected
        for f in expected:
            if f not in outcome:
                return outcome == expected
        for f in outcome:
            if f not in expected:
                return outcome == expected
        return True

    def remove_folder_items(outcome):
        # Remove folder-items from folders found
        if isinstance(outcome, list):
            for f in outcome:
                if f and f.get('folder-item'):
                    del f['folder-item']
        else:
            if outcome and outcome.get('folder-item'):
                del outcome['folder-item']

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        shutil.copyfile(f'resources/{folder}/test_live_data_source.tdsx', 'test_live_data_source.tdsx')
        shutil.copyfile(f'resources/{folder}/one_folder.tdsx', 'one_folder.tdsx')
        tds_extract = tableau_utilities.extract_tds('test_data_source.tdsx')
        tds_live = tableau_utilities.extract_tds('test_live_data_source.tdsx')
        tds_one_folder = tableau_utilities.extract_tds('one_folder.tdsx')
        os.remove('test_data_source.tdsx')
        os.remove('test_live_data_source.tdsx')
        os.remove('one_folder.tdsx')
        # The new xml structure does not specify role
        if folder == 'latest_xml_structure':
            expected_two_folders = [collections.OrderedDict({'@name': 'neat'}),
                                    collections.OrderedDict({'@name': 'tidy'})]
            expected_one_folder = [collections.OrderedDict({'@name': 'neat'})]
        else:
            expected_two_folders = [collections.OrderedDict({'@name': 'neat', '@role': 'measures'}),
                                    collections.OrderedDict({'@name': 'tidy', '@role': 'dimensions'})]
            expected_one_folder = [collections.OrderedDict({'@name': 'neat', '@role': 'measures'})]
        _folder = tableau_utilities.TDS(tds_extract).list('folder')
        _folder_live = tableau_utilities.TDS(tds_live).list('folder')
        _folder_one_folder = tableau_utilities.TDS(tds_one_folder).list('folder')
        remove_folder_items(_folder)
        remove_folder_items(_folder_live)
        remove_folder_items(_folder_one_folder)
        assert compare_list(outcome=_folder, expected=expected_two_folders)
        assert _folder_live is None
        assert _folder_one_folder == expected_one_folder


# TEST 15: find_column()
def test_find_column():

    for folder in ['latest_xml_structure', 'legacy_xml_structure']:
        shutil.copyfile(f'resources/{folder}/test_data_source.tdsx', 'test_data_source.tdsx')
        shutil.copyfile(f'resources/{folder}/test_live_data_source.tdsx', 'test_live_data_source.tdsx')
        tds_extract = tableau_utilities.extract_tds('test_data_source.tdsx')
        tds_live = tableau_utilities.extract_tds('test_live_data_source.tdsx')
        os.remove('test_data_source.tdsx')
        os.remove('test_live_data_source.tdsx')
        attribs = {'item_type': 'column', 'column_name': 'Number of Records'}
        assert tableau_utilities.TDS(tds_extract).get(**attribs) is not None
        assert tableau_utilities.TDS(tds_live).get(**attribs) is not None
