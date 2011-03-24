from setuptools import setup, find_packages

setup(
    name = "brewery",
    version = '0.6.0',

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = [],

    packages=find_packages(exclude=['ez_setup']),
    # packages = ['brewery'],
	# package_dir = { 'brewery': 'brewery'},
    package_data = {
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.rst'],
    },
    scripts = ['bin/mongoaudit', 'bin/brewery'],
    
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities'
    ],
    
    test_suite = "brewery.tests",

    # metadata for upload to PyPI
    author = "Stefan Urbanek",
    author_email = "stefan.urbanek@gmail.com",
    description = "Framework for processing, analysing and measuring quality of structured data streams",
    license = "GPL",
    keywords = "data analysis quality",
    url = "http://www.databrewery.org"

    # could also include long_description, download_url, classifiers, etc.
)
