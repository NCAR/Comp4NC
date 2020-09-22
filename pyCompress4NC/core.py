import glob
import logging
import os
import shutil
from os.path import abspath, basename, dirname, join
from time import sleep, time

import click
import dask.array as da
import numpy as np
import xarray as xr
import zarr
from dask_jobqueue import PBSCluster, SLURMCluster
from distributed import Client
from numcodecs.zfpy import ZFPY, _zfpy

logger = logging.getLogger()
logger.setLevel(level=logging.WARNING)

here = dirname(abspath(dirname(__file__)))
results_dir = join(here, 'results')


def cluster_wait(client, n_workers):
    """ Delay process until all workers in the cluster are available.
    """
    start = time()
    wait_thresh = 600
    worker_thresh = n_workers * 0.95

    while len(client.cluster.scheduler.workers) < n_workers:
        sleep(2)
        elapsed = time() - start
        # If we are getting close to timeout but cluster is mostly available,
        # just break out
        if elapsed > wait_thresh and len(client.cluster.scheduler.workers) >= worker_thresh:
            break


def convert_zarr(ds, varname, tol, path_zarr):

    compressor = ZFPY(mode=_zfpy.mode_fixed_accuracy, tolerance=tol)
    # zarr.storage.default_compressor = compressor

    ds1 = ds.chunk(chunks={'time': 100})
    ds1[varname].encoding['compressor'] = compressor

    ds1.to_zarr(path_zarr, mode='w', consolidated=True)


def write_to_netcdf(path_zarr, path_nc):

    ds = xr.open_zarr(path_zarr)
    comp = dict(zlib=True, complevel=5)
    encoding = {var: comp for var in ds.data_vars}

    ds.to_netcdf(path_nc, encoding=encoding)


def output_path(tol, cmp0, frequency, var, filename_first, write=False):

    dirout = ' '
    path_zarr = f'{dirout}/cmp0/{tol}/{frequency}/{filename_first}.zarr'
    path_nc = f'{dirout}/cmp0/{tol}/{frequency}/{filename_first}.nc'
    if write and os.path.exists(path_zarr):
        shutil.rmtree(path_zarr)
    print(path_zarr)
    return path_zarr, path_nc


def parse_filename(filename):

    import re

    'filename = /glade/p/cisl/asap/abaker/pepsi/ens_31/orig/monthly/*.nc'
    test_str = '/'
    res = [i.start() for i in re.finditer(filename, test_str)]
    cmp0 = res[5] + '/' + res[6]
    frequency = res[8]
    filename_only = res[-1]
    test_str = '.'
    res = [i.start() for i in re.finditer(filename_only, test_str)]
    filename_first = basename(filename)
    varname = res[7]
    print(varname)
    return varname, cmp0, frequency, filename_first


class Runner:
    def __init__(self, input_file):
        import yaml

        try:
            with open(input_file) as f:
                self.params = yaml.safe_load(f)
        except Exception as exc:
            raise exc
        self.client = None

    def create_cluster(self, queue, maxcore, memory, wpn, walltime):
        cluster = PBSCluster(
            queue=queue,
            cores=maxcore,
            memory=memory,
            processes=wpn,
            local_directory='$TMPDIR',
            walltime=walltime,
            # resource_spec='select=1:ncpus=36:mem=109GB',
        )
        logger.warning(memory)
        logger.warning(walltime)
        # resource_spec='select=1:ncpus=36:mem=109GB')
        logger.warning(cluster.job_script())
        self.client = Client(cluster)

    def run(self):
        logger.warning('Reading configuration YAML config file')
        queue = self.params['queue']
        walltime = self.params['walltime']
        memory = self.params['memory']
        maxcore_per_node = self.params['maxcore_per_node']
        num_workers = self.params['number_of_workers_per_nodes']
        num_nodes = self.params['number_of_nodes']
        input_file = self.params['input_dir']
        output_dir = self.params['output_dir']
        logger.warning(memory)
        logger.warning(maxcore_per_node)
        logger.warning(num_nodes)
        logger.warning(num_workers)
        logger.warning(input_file)
        logger.warning(output_dir)
        logger.warning(walltime)
        self.create_cluster(queue, maxcore_per_node, memory, num_workers, walltime)
        self.client.cluster.scale(n=num_nodes * num_workers)
        logger.warning('scale')
        # sleep(10)
        self.client.wait_for_workers(num_nodes * num_workers)
        logger.warning('wait')
        # cluster_wait(self.client, num_nodes * num_workers)

        files = glob.glot('/glade/p/cisl/asap/abaker/pepsi/ens_31/orig/monthly/*.nc')
        tol = 1e-2
        # dirout = '/glade/scratch/haiyingx'
        for i in files:
            varname, cmp0, frequency, filename_first = parse_filename(i)
            ds = xr.open_dataset(i)
            path_zarr, path_nc = output_path(tol, cmp0, frequency, varname, filename_first)
            convert_zarr(ds, varname, tol, path_zarr)
            write_to_netcdf(path_zarr, path_nc)
        # ds = xr.open_dataset(
        #    input_file, chunks={'nVertLevels': 1, 'nCells': 2621442, 'nVertLevelsP1': 1}
        # )

        logger.warning(ds)
        logger.warning('done')
        self.client.cluster.close()
        self.client.close()
        return ds
