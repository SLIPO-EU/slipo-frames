import os
import io

from setuptools import setup, find_packages

metadata = {}

# Required modules
install_requires = [
    'jinja2>=2.10',
    'pandas>=0.24.2',
    'slipo>=0.1.4',
]

extras_require = {
    ':python_version >= "3.4"': [
        'IPython>=6.0.0'
    ],
}

# Get working directory
cur_dir = os.path.abspath(os.path.dirname(__file__))

# Get README content
with io.open(os.path.join(cur_dir, 'README.md'), mode='r', encoding='utf-8') as f:
    long_description = '\n' + f.read()

# Get project version
with io.open(os.path.join(cur_dir, 'slipoframes', '__version__.py'), mode='r', encoding='utf-8') as f:
    exec(f.read(), metadata)

PACKAGE_DATA = {
    'slipoframes': [
        'templates/*',
        'templates/css/*',
        'templates/js/*',
    ],
}

setup(
    name='slipoframes',
    version=metadata['__version__'],
    author='Yannis Kouvaras',
    author_email='jkouvar@imis.athena-innovation.gr',
    license='Apache Software License',
    description='SLIPO API client for Jupyter notebooks',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/SLIPO-EU/slipo-frames',
    packages=find_packages(),
    python_requires='>=3',
    extras_require=extras_require,
    classifiers=[
        'Programming Language :: Python :: 3',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
    ],
    install_requires=install_requires,
    keywords='poi linked-data point-of-interest data-integration jupyter-notebook',
    test_suite='nose.collector',
    tests_require=['nose'],
    package_data=PACKAGE_DATA,
)
