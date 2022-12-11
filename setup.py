from setuptools import setup

with open('README.md') as f:
    readme = f.read()

setup(
    author="Justin Grilli",
    author_email="jgrilli@hover.to",
    license='MIT',
    url='http://pypi.python.org/pypi/tableau-utilities/',
    description='Utility for maintaining Tableau objects',
    long_description=readme,
    long_description_content_type='text/markdown',
    name="tableau_utilities",
    version="2.0.0",
    packages=[
        'tableau_utilities',
        'tableau_utilities.general',
        'tableau_utilities.tableau_file',
        'tableau_utilities.tableau_server'
    ],
    package_data={'tableau_utilities': ['tableau_file/*.yml']},
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'tableau_utilities = tableau_utilities.main:main',
            'tableau_utilities_config_gen = tableau_utilities.datasources_column_config_generate:main'
            'tableau_utilities_config_merge = tableau_utilities.datasources_column_config_merge:main'
            'tableau_utilities_server = tableau_utilities.server:main'
        ]
    }
)
