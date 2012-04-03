from setuptools import setup, find_packages

install_requires = []

try:
    # For Python >= 2.6
    import json
except ImportError:
    # For Python < 2.6 or people using a newer version of simplejson
    install_requires.append("simplejson")
    
setup(
    name = "brewery",
    version = '0.8.0',

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires=install_requires,

    packages=find_packages(exclude=['ez_setup']),
    # packages = ['brewery'],
	# package_dir = { 'brewery': 'brewery'},
    package_data={
        # If any package contains *.txt or *.rst files, include them:
        '': ['*.txt', '*.rst'],
    },

    scripts=['bin/mongoaudit', 'bin/brewery'],

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Topic :: Database',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities'
    ],

    test_suite="brewery.tests",

    # metadata for upload to PyPI
    author="Stefan Urbanek",
    author_email="stefan.urbanek@gmail.com",
    description="Framework for analysing data and measuring quality of data using structured data streams",
    license="MIT",
    keywords="data analysis quality datamining",
    url="http://www.databrewery.org"

    # could also include long_description, download_url, classifiers, etc.
)
