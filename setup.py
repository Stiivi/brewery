from setuptools import setup, find_packages

setup(
    name = "brewery",
    version = '0.5.0',

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = [],

    packages = ['brewery'],
	package_dir = { 'brewery': 'brewery'},
    package_data = {
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.rst'],
    },
    scripts = ['bin/brewaudit'],
    
    test_suite = "brewery.tests.test_model",

    # metadata for upload to PyPI
    author = "Stefan Urbanek",
    author_email = "stefan.urbanek@gmail.com",
    description = "Data analysis framework",
    license = "GPL",
    keywords = "data analysis olap quality",
    url = "http://www.databrewery.org"

    # could also include long_description, download_url, classifiers, etc.
)
