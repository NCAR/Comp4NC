.. image:: https://img.shields.io/github/workflow/status/NCAR/pyCompress4NC/CI?logo=github&style=for-the-badge
    :target: https://github.com/NCAR/pyCompress4NC/actions
    :alt: GitHub Workflow CI Status

.. image:: https://img.shields.io/github/workflow/status/NCAR/pyCompress4NC/code-style?label=Code%20Style&style=for-the-badge
    :target: https://github.com/NCAR/pyCompress4NC/actions
    :alt: GitHub Workflow Code Style Status

.. image:: https://img.shields.io/codecov/c/github/NCAR/pyCompress4NC.svg?style=for-the-badge
    :target: https://codecov.io/gh/NCAR/pyCompress4NC

.. If you want the following badges to be visible, please remove this line, and unindent the lines below
    .. image:: https://img.shields.io/readthedocs/pyCompress4NC/latest.svg?style=for-the-badge
        :target: https://pyCompress4NC.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

    .. image:: https://img.shields.io/pypi/v/pyCompress4NC.svg?style=for-the-badge
        :target: https://pypi.org/project/pyCompress4NC
        :alt: Python Package Index

    .. image:: https://img.shields.io/conda/vn/conda-forge/pyCompress4NC.svg?style=for-the-badge
        :target: https://anaconda.org/conda-forge/pyCompress4NC
        :alt: Conda Version


Comp4NC
=============
Python tool to compress data with zfp to Zarr and convert to NetCDF

Production
------------

For a production install, do the following in the repository directory:

.. code-block:: bash

    git clone https://github.com/NCAR/Comp4NC.git
    cd Comp4NC
    conda env update -f ci/environment.yml
    conda activate pyCompress4NC
    python -m pip install -e .

Before run the codes::

    export PBS_ACCOUNT=youraccount #in .bash_profile
    setenv PBS_ACCOUNT youraccount #in .tcshrc

To run the codes with config file as input::

    ./pyCompress --config_file pyCompress4NC/cheyenne.yaml


To get help with command lines::

    ./pyCompress --help

Command line options::

    Options:
      --config_file PATH              Yaml file to set all input parameters, can
                                      replace command line parameters

      --queue TEXT                    Cluster queue name  [default: regular]
      --walltime TEXT                 Expected walltime of the job  [default:
                                      1:00:00]

      --memory TEXT                   Expected memory needed on each computing
                                      node  [default: 109GB]

      --maxcore_per_node INTEGER      Core count on each computing node  [default:
                                      36]

      --number_of_workers_per_nodes INTEGER
                                      Number of cores required on each node
                                      [default: 10]

      --number_of_nodes INTEGER       Number of nodes required  [default: 1]
      --to_nc                         Writing to netcdf format  [default: False]
      --parallel                      Running in parallel mode  [default: False]

      --input_file PATH               Input file name
      --input_dir PATH                Input directory name
      --output_dir PATH               Output directory name  [default: .]
      --start INTEGER                 Start index of the file in a file list
                                      [default: 0]

      --end INTEGER                   End index of the file in a file list
                                      [default: 1]

      --comp_method TEXT              Specify which compression method will be
                                      used on time variant variable.  [default:
                                      zfp]

      --comp_mode TEXT                Specify which compression mode (a, p, r) will be used.
                                      [default: a]

      --comp_level FLOAT              Specify which compression level will be
                                      used.  [default: 0.1]

      --chunkable_dimension TEXT      Specify the chunk dimension for some
                                      variables.  [default: {}]

      --chunkable_chunksize INTEGER   Specify the chunk size for
                                      chunkable_dimension.  [default: 10]

      --help                          Show this message and exit.

To run the codes from command lines with compression zfp in p mode, keeping 16 bits and outputing to netcdf format parallelly::

    ./pyCompress --input_file /glade/p/cisl/asap/ldcpy_sample_data/lens/orig/
    TS.daily.20060101-20801231.nc --comp_method zfp --comp_mode p
    --comp_level 16 --parallel --to_nc --chunkable_dimension time

Re-create notebooks with Pangeo Binder
--------------------------------------

Try notebooks hosted in this repo on Pangeo Binder. Note that the session is ephemeral.
Your home directory will not persist, so remember to download your notebooks if you
made changes that you need to use at a later time!

.. image:: https://img.shields.io/static/v1.svg?logo=Jupyter&label=Pangeo+Binder&message=GCE+us-central1&color=blue&style=for-the-badge
    :target: https://binder.pangeo.io/v2/gh/NCAR/pyCompress4NC/master?urlpath=lab
    :alt: Binder
