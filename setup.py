from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    url="https://github.com/hoverinc/tableau-utilities",
    author="Justin Grilli",
    author_email="jgrilli@hover.to",
    description='Utility for maintaining Tableau objects',
    long_description=readme,
    name="tableau_utilities",
    version="1.0.0",
    packages=find_packages(),
    package_data={"tableau_utilities": ["item_types.yml"]},
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'tableau_utilities = tableau_utilities:main'
        ]
    }
)
