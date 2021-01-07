import dask.array as da
import xarray as xr
import zarr


def get_missingval_mask(ds):
    null_arr_3d = None
    null_arr_4d = None
    for var in ds.data_vars:
        print(var)
        if len(ds[var].dims) == 3 and ds[var].dtype == 'float32':
            null_arr_3d = ds[var][0].isnull()
            d = ds[var].fillna(0)
            ds[var].data = d.data
        elif len(ds[var].dims) == 4 and ds[var].dtype == 'float32':
            null_arr_4d = ds[var][0].isnull()
            d = ds[var].fillna(0)
            ds[var].data = d.data
    if null_arr_3d is not None:
        ds1 = ds.assign(missing_mask_3d=null_arr_3d)
        ds = ds1
    if null_arr_4d is not None:
        ds1 = ds.assign(missing_mask_4d=null_arr_4d)
        ds = ds1
    return ds


def apply_missingval(ds, varname3d, varname4d):
    for i in varname3d:
        dv = ds[i].where(ds['missing_mask_3d'] == 0)
        ds[i].data = dv.data
    for i in varname4d:
        dv = ds[i].where(ds['missing_mask_4d'] == 0)
        ds[i].data = dv.data
    return ds


def open_zarrfile(filename):

    var3d_dict = []
    var4d_dict = []
    ds = xr.open_zarr(filename)
    for var in ds.data_vars:
        if len(ds[var].dims) == 3 and ds[var].dtype == 'float32':
            var3d_dict.append(var)
        elif len(ds[var].dims) == 4 and ds[var].dtype == 'float32':
            var4d_dict.append(var)
    ds1 = apply_missingval(ds, var3d_dict, var4d_dict)
    return ds1


def assert_orig_recon(file_orig, file_recon, chunkable_dim, POP):
    if POP:
        ds_recon = open_zarrfile(file_recon)
    else:
        ds_recon = xr.open_zarr(file_recon)
    ds_orig = xr.open_dataset(file_orig, chunks=chunkable_dim)
    for var in ds_orig.data_vars:
        if len(ds_orig[var].dims) >= 2 and ds_orig[var].dtype == 'float32':
            d_orig = ds_orig[var]
            d_recon = ds_recon[var]
            validate = da.allclose(d_recon, d_orig, rtol=0.0, atol=0.01, equal_nan=True)
            print(var, 'validate', validate.compute())
