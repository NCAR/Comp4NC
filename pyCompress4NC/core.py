import glob
import logging
import re
import shutil
from math import ceil
from os import walk
from os.path import abspath, basename, dirname, exists, getsize, join
from pathlib import Path
from time import sleep, time
from timeit import default_timer as timer

import click
import dask.array as da
import numpy as np
import xarray as xr
import yaml
import zarr
from dask_jobqueue import PBSCluster, SLURMCluster
from distributed import Client
from numcodecs.quantize import Quantize
from numcodecs.zfpy import ZFPY, _zfpy
from numcodecs.zlib import Zlib

from .process_missingval import (
    assert_orig_recon,
    get_missingval_mask,
    open_zarrfile,
    reorder_mpas_data,
)

logger = logging.getLogger()
logger.setLevel(level=logging.WARNING)

here = dirname(abspath(dirname(__file__)))
results_dir = join(here, 'results')


def cluster_wait(client, n_workers):
    """Delay process until all workers in the cluster are available."""
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


def zfp_compressor(varname, comp):
    if comp['comp_mode'] == 'a':
        m = _zfpy.mode_fixed_accuracy
        compressor = ZFPY(mode=m, tolerance=float(comp['comp_level']))
    elif comp['comp_mode'] == 'p':
        m = _zfpy.mode_fixed_precision
        compressor = ZFPY(mode=m, precision=int(comp['comp_level']))
    elif comp['comp_mode'] == 'r':
        m = _zfpy.mode_fixed_rate
        compressor = ZFPY(mode=m, rate=int(comp['comp_level']))
    else:
        print('Wrong zfp compression mode')
    return compressor


def zlib_compressor(varname, comp):

    compressor = Zlib(level=comp['comp_level'])
    return compressor


def quantize_compressor(varname, comp):
    compressor = Quantize(digits=comp['comp_level'], dtype='f4', astype='f2')
    return compressor


def define_compressor(varname, comp_dict):

    compressor = {}
    if 'all' in comp_dict:
        comp = comp_dict['all']
    else:
        if varname in comp_dict:
            comp = comp_dict[varname]
        else:
            print("Error, you didn't define compression method for " + varname)
            exit()

    if comp['comp_method'] == 'zfp':
        compressor[varname] = zfp_compressor(varname, comp)
    else:
        compressor[varname] = zlib_compressor(varname, comp)

    return compressor


def convert_to_zarr(POP, ds, varname, chunkable_dim, path_zarr, comp, na, client):

    if 'time' in chunkable_dim:
        """time series file only has one variable to compress"""
        """ and we can use time diemnsion as chunking dimension """
        for _varname in ds.data_vars:
            if len(ds[_varname].dims) >= 2 and ds[_varname].dtype == 'float32':
                timestep = calculate_chunks(ds, _varname)
                varname = _varname
        zarr.storage.default_compressor = Zlib(level=5)
        compressor = define_compressor(varname, comp)
        if bool(na):
            ds1 = get_missingval_mask(ds, POP, na)
            ds = ds1
        ds1 = ds.chunk(chunks={'time': timestep})
        ds1[varname].encoding['compressor'] = compressor[varname]
        ds1.to_zarr(path_zarr, mode='w', consolidated=True)

    else:
        """time history file has many variables to compress"""
        """ and we need users to specify chunking dimensions from config file """
        ds1 = ds.chunk(chunks=chunkable_dim)
        for _varname in ds.data_vars:
            if len(ds[_varname].dims) >= 2 and ds[_varname].dtype == 'float32':
                compressor = define_compressor(_varname, comp)
                ds1[_varname].encoding['compressor'] = compressor[_varname]
                # ds1[_varname].encoding['compressor'] = None
                # a = ds1[_varname].data.map_blocks(zarr.array, compressor=compressor[_varname]).persist().map_blocks(np.array)
                # ds1[_varname].data = a
                # reorder_mpas_data(ds, _varname, client, compressor, path_zarr)
        ds1.to_zarr(path_zarr, mode='w', consolidated=True)


def calculate_chunks(ds, varname):
    """Check if variable data nbytes are less than 192MB"""
    if ds[varname].nbytes < 201326592:
        timestep = -1
    else:
        x = ceil(ds[varname].nbytes / 201326592)
        timestep = int(ds[varname].sizes['time'] / x)
    return timestep


def write_to_netcdf(path_zarr, path_nc, POP, split_nc):

    # if POP:
    ds = open_zarrfile(path_zarr)
    # else:
    #    ds = xr.open_zarr(path_zarr)
    comp = dict(zlib=True, complevel=5)
    encoding = {}
    for var in ds.data_vars:
        encoding[var] = {}
        if len(ds[var].dims) >= 2 and ds[var].dtype == 'float32':
            # print(var, ds[var].encoding['chunks'])
            if split_nc:
                ds[var].encoding['chunksizes'] = list(ds[var].encoding['chunks'])
                ds[var].encoding.update(comp)
                ds[var].to_netcdf(path_nc[:-2] + var + '.nc')
            else:
                encoding[var]['chunksizes'] = list(ds[var].encoding['chunks'])
                encoding[var].update(comp)
        else:
            encoding[var] = comp

    if not split_nc:
        ds.to_netcdf(path_nc, encoding=encoding)
    shutil.rmtree(path_zarr)


def output_singlefile_path(filename_dir, var, filename_first, dirout, comp_dict, write=False):

    if 'all' in comp_dict:
        comp = comp_dict['all']
        comp_name = f'{comp["comp_method"]}_{comp["comp_mode"]}_{comp["comp_level"]}'
    else:
        comp_name = 'hybrid'
    path_zarr = f'{dirout}/{comp_name}/{filename_first}.zarr'
    path_nc = f'{dirout}/{comp_name}/{filename_first}.nc'
    s3 = False
    if s3:
        import fsspec

        fs = fsspec.filesystem(
            's3',
            profile='default',
            anon=False,
            client_kwargs={'endpoint_url": "https://stratus.ucar.edu'},
            skip_instance_cache=True,
            use_listings_cache=True,
        )
        root = 'comp4nc-files'
        path_zarr = fs.get_mapper(
            root=f'{root}/{comp_name}/{filename_first}.zarr', check=False, create=True
        )
    # if write and exists(path_zarr):
    #    shutil.rmtree(path_zarr)
    return path_zarr, path_nc


def output_path(cmpn, frequency, var, filename_first, dirout, comp, write=False):
    comp_name = f'{comp["comp_method"]}_{comp["comp_mode"]}_{comp["comp_level"]}'
    path_zarr = f'{dirout}/{cmpn}/{comp_name}/{frequency}/{filename_first}.zarr'
    path_nc = f'{dirout}/{cmpn}/{comp_name}/{frequency}/{filename_first}.nc'
    if write and exists(path_zarr):
        shutil.rmtree(path_zarr)
    return path_zarr, path_nc


def parse_singlefile(filename):
    filename_only = basename(filename)
    if 'pop' in filename_only:
        POP = True
    else:
        POP = False
    res = re.split(r'\.', filename_only)
    varname = res[0]
    period = res[2]
    filename_first = filename_only[:-3]
    filename_dir = dirname(filename)

    return POP, varname, period, filename_dir, filename_first


def parse_filename(filename):
    if 'pop' in filename:
        POP = True
    else:
        POP = False

    res = re.split(r'\/', filename)
    cmpn = res[6] + '/' + res[7]
    frequency = res[9]
    filename_only = basename(filename)
    res = re.split(r'\.', filename_only)
    filename_first = filename_only[:-3]
    varname = res[7]
    period = res[8]

    return POP, varname, period, cmpn, frequency, filename_first


def get_filesize(file_dict, period, to_nc):

    for k, v in file_dict.items():
        origsize = getsize(file_dict['orig'])
        filesize = 0
        if k == 'zarr' and not to_nc:
            """Parse zarr info to get compressed file size"""
            filesize = sum(p.stat().st_size for p in Path(v).rglob('*'))

            print(f'{basename(file_dict["orig"])} input size {origsize}')
            print(f'{basename(file_dict["zarr"])} {k} size {filesize}')
        elif k == 'nc' and to_nc:
            filesize = getsize(v)
            print(f'{basename(file_dict["orig"])} input size {origsize}')
            print(f'{basename(file_dict["nc"])} {k} size {filesize}')


class Runner:
    def __init__(self, **context_dict):

        if context_dict['config_file']:

            try:
                with open(context_dict['config_file']) as f:
                    self.params = yaml.safe_load(f)
            except Exception as exc:
                raise exc
        else:
            self.params = context_dict
            compression = {}
            compression['all'] = {}
            compression['all']['comp_method'] = context_dict['comp_method']
            compression['all']['comp_mode'] = context_dict['comp_mode']
            compression['all']['comp_level'] = context_dict['comp_level']
            self.params['compression'] = compression
            index_of_files = {}
            index_of_files['start'] = context_dict['start']
            index_of_files['end'] = context_dict['end']
            self.params['index_of_files'] = index_of_files
            if context_dict['chunkable_dimension']:
                chunkable = {}
                chunkable[context_dict['chunkable_dimension']] = context_dict['chunkable_chunksize']
                self.params['chunkable_dimension'] = chunkable
        self.client = None

    def create_cluster(self, queue, maxcore, memory, wpn, walltime):
        cluster = PBSCluster(
            queue=queue,
            cores=maxcore,
            memory=memory,
            processes=wpn,
            local_directory='$TMPDIR',
            walltime=walltime,
            # extra=['--nthreads', '1', '--lifetime', '55m', '--lifetime-stagger', '4m'],
            # resource_spec='select=1:ncpus=12:ompthreads=12:mem=109GB',
        )
        logger.warning(cluster.job_script())
        self.client = Client(cluster)

    def run(self):
        # logger.warning('Reading configuration YAML config file')
        queue = self.params['queue']
        walltime = self.params['walltime']
        memory = self.params['memory']
        logger.warning(memory)
        maxcore_per_node = self.params['maxcore_per_node']
        num_workers = self.params['number_of_workers_per_nodes']
        num_nodes = self.params['number_of_nodes']
        LENS = False
        parallel = self.params['parallel']
        to_nc = self.params['to_nc']
        split_nc = self.params['split_nc']
        input_file = self.params['input_file']
        input_dir = self.params['input_dir']
        output_dir = self.params['output_dir']
        num_files = self.params['index_of_files']
        fill_nan_value = {}
        if 'fill_nan_value' in self.params:
            fill_nan_value = self.params['fill_nan_value']
        chunkable_dim = self.params['chunkable_dimension']
        if 'compression' not in self.params:
            try:
                with open(self.params['compress_config']) as fc:
                    compression = yaml.safe_load(fc)
            except Exception as exc:
                raise exc
            # print(compression)
        else:
            compression = self.params['compression']

        # logger.warning(memory)
        # logger.warning(maxcore_per_node)
        # logger.warning(num_nodes)
        # logger.warning(num_workers)
        # logger.warning(input_dir)
        # logger.warning(output_dir)
        # logger.warning(walltime)
        if parallel:
            self.create_cluster(queue, maxcore_per_node, memory, num_workers, walltime)
            self.client.cluster.scale(n=num_nodes * num_workers)
            logger.warning('scale')
            self.client.wait_for_workers(n_workers=num_nodes * num_workers)
            logger.warning('wait')

        start_time = timer()
        if input_file is None:
            files = glob.glob(input_dir + '/*.nc')
        else:
            files = [input_file]
        pre = {}
        for counter, i in enumerate(files):
            if (counter >= num_files['start']) and (counter < num_files['end']):
                if LENS:
                    (
                        POP,
                        varname,
                        period,
                        cmpn,
                        frequency,
                        filename_first,
                    ) = parse_filename(i)
                else:
                    (
                        POP,
                        varname,
                        period,
                        filename_dir,
                        filename_first,
                    ) = parse_singlefile(i)

                with xr.open_dataset(i, chunks=chunkable_dim) as ds:
                    if LENS:
                        path_zarr, path_nc = output_path(
                            cmpn,
                            frequency,
                            varname,
                            filename_first,
                            output_dir,
                            compression,
                        )
                    else:
                        path_zarr, path_nc = output_singlefile_path(
                            filename_dir,
                            varname,
                            filename_first,
                            output_dir,
                            compression,
                        )
                    pre['var'] = varname
                    pre['orig'] = i
                    pre['zarr'] = path_zarr
                    pre['nc'] = path_nc
                    convert_to_zarr(
                        POP,
                        ds,
                        varname,
                        chunkable_dim,
                        path_zarr,
                        compression,
                        fill_nan_value,
                        self.client,
                    )
                    assert_orig_recon(i, path_zarr, chunkable_dim, fill_nan_value)
                    print(i, '... Done')
                    if to_nc:
                        write_to_netcdf(path_zarr, path_nc, POP, split_nc)
                    # get_filesize(pre, period, to_nc)

        # logger.warning(ds)
        logger.warning('All done')
        end_time = timer()
        print('elapsed time', end_time - start_time)
        if parallel:
            self.client.cluster.close()
            self.client.close()
        return ds
