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
    version="1.0.4",
    packages=['tableau_utilities'],
    package_data={"tableau_utilities": ["item_types.yml"]},
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'tableau_utilities = tableau_utilities:main'
        ]
    }
)
