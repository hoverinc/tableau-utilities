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
        'tableau_utilities.tableau_server',
        'tableau_utilities.scripts'
    ],
    package_data={'tableau_utilities': ['tableau_file/*.yml']},
    include_package_data=True,
    install_requires=['xmltodict>=0.12.0',
                      'pyyaml>=6.0',
                      'requests>=2.27.1',
                      'pandas>=1.4.1',
                      'tabulate>=0.8.9',
                      'pytest>==7.1.2'],
    entry_points={
        'console_scripts': [
            'tableau_utilities = tableau_utilities.scripts.cli:main',
        ]
    }
)
