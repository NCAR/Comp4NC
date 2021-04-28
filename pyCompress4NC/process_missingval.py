import shutil
from os.path import exists
from pathlib import Path

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr


def get_missingval_mask(ds, pop, na):
    print('get_missingval_mask')
    null_arr = None
    for var in ds.data_vars:
        print(var, ds[var].dims)

        if len(ds[var].dims) >= 3 and ds[var].dtype == 'float32' and ds[var].dims[0] == 'time':
            if pop:
                print(ds[var].data.shape)
                null_arr = ds[var][0].isnull()
            else:
                null_arr = ds[var].isnull()
                if not null_arr.any():
                    continue
            print('after continue')
            if na['api'] == 'interp':
                d = ds[var].interpolate_na(na['dim'], method=na['method'])
            elif na['api'] == 'fillna':
                d = ds[var].fillna(na['fill_value'])
            ds[var].data = d.data
            var_name = f'missing_mask_{var}'
            ds[var_name] = null_arr
    return ds


def apply_missingval(ds, varname_dict):
    for i in varname_dict:
        dv = ds[i].where(ds[f'missing_mask_{i}'] == 0)
        ds[i].data = dv.data
    return ds


def open_zarrfile(filename):

    var_dict = []
    ds = xr.open_zarr(filename)
    for var in ds.data_vars:
        if len(ds[var].dims) >= 3 and ds[var].dtype == 'float32':
            if f'missing_mask_{var}' in ds.data_vars:
                var_dict.append(var)
    if bool(var_dict):
        ds1 = apply_missingval(ds, var_dict)
    else:
        print('no')
        ds1 = ds
    return ds1


def assert_orig_recon(file_orig, file_recon, chunkable_dim, na):
    if bool(na):
        ds_recon = open_zarrfile(file_recon)
    else:
        ds_recon = xr.open_zarr(file_recon)
    # var_dict = []
    # miss_arr = 'missing_mask_'
    # for var in ds.data_vars:
    #    if miss_arr in var:
    #        varname = var[len(miss_arr):]
    #        var_dict.append(varname)
    # if bool(var_dict):
    #    ds_recon = apply_missingval(ds, var_dict)
    # else:
    #    print('assert')
    #    ds_recon = ds
    ds_orig = xr.open_dataset(file_orig, chunks=chunkable_dim)
    for var in ds_orig.data_vars:
        if len(ds_orig[var].dims) >= 3 and ds_orig[var].dtype == 'float32':
            d_orig = ds_orig[var]
            d_recon = ds_recon[var]
            orig_mean = d_orig.mean().persist()
            orig_max = d_orig.max().persist()
            orig_min = d_orig.min().persist()
            recon_mean = d_recon.mean().persist()
            recon_max = d_recon.max().persist()
            recon_min = d_recon.min().persist()
            # validate = da.allclose(d_recon, d_orig, rtol=0.0, atol=0.01, equal_nan=True)
            print(
                var,
                'orig mean: ',
                orig_mean.values,
                orig_max.values,
                orig_min.values,
                ' recon mean: ',
                recon_mean.values,
                recon_max.values,
                recon_min.values,
            )


# def assert_average_orig_recon(ds_var, output_csv):
#    max_val = d_var.max(dim='time')
#    min_val = d_var.min(dim='time')
#    mean_val = d_var.mean(dim='time')


def reorder_mpas_data(ds, var, client, comp, path_zarr):
    nCells = 41943042
    perm_arr = np.fromfile(f'/glade/work/haiyingx/mpas_655362/mc2gv.dat.{nCells}', dtype='i4')
    print(perm_arr.shape)
    [future] = client.scatter([perm_arr], broadcast=True)
    arr_shape = ds[var].data.shape
    print(var, ds[var].dims, arr_shape)
    if len(ds[var].dims) == 3:
        var_arr = da.transpose(ds[var].data, (0, 2, 1))
    else:
        var_arr = ds[var].data
    arr_size = var_arr.nbytes
    """Using Ellipsis ... here to deal with both 2D and 3D variables"""
    reindex_arr = da.map_blocks(lambda x, y: x[..., y], var_arr, perm_arr, dtype='f4')
    """Only pad the last dimension"""
    padded_tuple = ((0, 0),) * (len(ds[var].dims) - 1) + ((0, 2046),)
    padded_arr = da.pad(reindex_arr, padded_tuple, 'constant')
    print('var', var, padded_tuple)
    # arr = padded_arr.reshape(padded_arr.shape[0],padded_arr.shape[1],-1,2048)
    arr = padded_arr.reshape(padded_arr.shape[:-1] + (20481, 2048))
    print(padded_arr.shape[:-1])
    """Use persist() can save in the memory and speed up when call compute()"""
    pre_b = arr.mean().persist()
    print(arr.shape)
    encoding = {f'{var}': {'compressor': comp[var]}}
    ds = xr.DataArray(arr, name=f'{var}').to_dataset()
    filename = f'{path_zarr[:-4]}{var}.zarr'
    if exists(filename):
        shutil.rmtree(filename)
    ds.to_zarr(filename, encoding=encoding)

    """Read the compressed file to get mean(), and compare abs tol"""
    filesize = sum(p.stat().st_size for p in Path(filename).rglob('*'))
    decomp_f = xr.open_zarr(filename)
    decomp_arr = decomp_f[var].data
    print(comp[var])
    if comp[var].codec_id == 'zfpy':
        tol = comp[var].tolerance
    else:
        tol = comp[var].level
    a = da.allclose(decomp_arr, arr, rtol=0.0, atol=tol).persist()
    b = decomp_f[var].mean().persist()

    """Save metric info to csv file"""
    results = []
    res_dict = {}
    res_dict['var_name'] = var
    res_dict['orig_size'] = arr_size
    res_dict['recon_size'] = filesize
    res_dict['ratio'] = round(arr_size / filesize, 2)
    res_dict['abs_valid'] = a.compute()
    res_dict['orig_avg'] = f'{pre_b.compute():.7f}'
    res_dict['recon_avg'] = f'{b.compute().values:.7f}'
    results.append(res_dict)
    pd.DataFrame(results).to_csv(
        '/glade/scratch/haiyingx/Falko/hybrid/size.txt',
        index=False,
        sep=',',
        mode='a',
        header=False,
    )
    # print(var, 'valid ', a.compute(), pre_b.compute(),b.compute())
