import xarray as xr
xr.set_options(keep_attrs = True)
import os
from . import cmor

def load_execute_save(func, data, version_id, data_init_path, chunks=None, *func_args, **func_kwargs):
    if type(data) == str:
        data = xr.open_dataset(data, use_cftime=True, chunks = chunks)
    data_proc = func(data,*func_args, **func_kwargs)
    data_cmor_path, data_filename = cmor.gen_cmor_path_and_filename(data_proc, version_id = version_id)
    data_full_path = os.path.join(data_init_path, data_cmor_path)
    os.makedirs(data_full_path, exist_ok=True)

    data_full_filename =os.path.join(data_full_path, data_filename) 
    data_proc.to_netcdf(data_full_filename)
    return data_full_filename
