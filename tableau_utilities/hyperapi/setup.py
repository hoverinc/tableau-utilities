from setuptools import setup

with open('../../README.md') as f:
    readme = f.read()

setup(
    author="Justin Grilli",
    author_email="justin.grilli@gmail.com",
    license='MIT',
    url='http://pypi.python.org/pypi/tableau-utilities-hyper/',
    description='Namespace Package for manipulated hyper packages to be used in conjunction with tableau-utilities ',
    long_description=readme,
    long_description_content_type='text/markdown',
    name="tableau_utilities_hyper",
    version="",
    install_requires=['tableau-utilities==2.2',
                      'tableauhyperapi==0.0.18825'],
)
