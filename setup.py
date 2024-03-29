# coding: utf-8
from setuptools import setup, find_packages
import codecs
from os import path
import io
import re

with io.open("reki_data_tool/__init__.py", "rt", encoding="utf8") as f:
    version = re.search(r'__version__ = "(.*?)"', f.read()).group(1)

here = path.abspath(path.dirname(__file__))

with codecs.open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='reki-data-tool',

    version=version,

    description='A data tool using cemc-oper/reki.',
    long_description=long_description,
    long_description_content_type='text/markdown',

    url='https://github.com/cemc-oper/reki-data-tool',

    author='perillaroc',
    author_email='perillaroc@gmail.com',

    license='MIT',

    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],

    keywords='cemc data grib2',

    packages=find_packages(exclude=['docs', 'tests', 'example']),

    include_package_data=True,

    install_requires=[
        "pyyaml",
        "jinja2",
        "numpy",
        "pandas",
        "xarray",
        "eccodes",
        "dask",
        "click",
        "tqdm",
        "loguru",
        "typer",
    ],

    extras_require={
        "cfgrib": [
            "cfgrib",
        ],
        "ml": [
            "scikit-learn"
        ],
        "test": ['pytest'],
        "cov": ['pytest-cov', 'codecov'],
    },

    entry_points={
        "console_scripts": [
        ],
    }
)
