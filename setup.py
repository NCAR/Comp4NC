#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

with open('requirements.txt') as f:
    requirements = f.read().strip().split('\n')

with open('README.rst') as f:
    long_description = f.read()

setup(
    maintainer='Haiying Xu',
    maintainer_email='haiyingx@ucar.edu',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Scientific/Engineering',
        'Operating System :: OS Independent',
        'Intended Audience :: Science/Research',
    ],
    description='Compress netcdf4 data to netcdf4/zarr with zfp compressor',
    install_requires=requirements,
    license='Apache Software License 2.0',
    long_description=long_description,
    include_package_data=True,
    keywords='pyCompress4NC',
    name='pyCompress4NC',
    packages=find_packages(include=['pyCompress4NC', 'pyCompress4NC.*']),
    url='https://github.com/NCAR/pyCompress4NC',
    project_urls={
        'Documentation': 'https://github.com/NCAR/pyCompress4NC',
        'Source': 'https://github.com/NCAR/pyCompress4NC',
        'Tracker': 'https://github.com/NCAR/pyCompress4NC/issues',
    },
    zip_safe=False,
)
