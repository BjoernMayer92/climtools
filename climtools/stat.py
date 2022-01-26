import xarray as xr
import numpy as np
from . import temporal

def gen_seasonal_cycle(start, end, frequency):
    """Generates an artifical seasonal cycle for times between a given start and endpoint with a given frequncy

    Args:
        start (cftime.Datetime): Beginning of the period
        end (cftime.Datetime): End of the period
        frequency (string): 

    Returns:
        xarray.Dataset: Dataset containing seasonal cycle
    """
    
    assert frequency in temporal.temporal_resolution_dict, "Frequency must be one the following: "+str(list(temporal.temporal_resolution_dict.keys()))
    frequency_str = temporal.temporal_resolution_dict[frequency]

    times_bnds_min = xr.cftime_range(start=start, end=end,  freq=frequency_str, calendar="proleptic_gregorian")[:-1]
    times_bnds_max = times_bnds_min.shift(1,frequency_str)

    times_stmp = temporal.cal_middle_time(times_bnds_min, times_bnds_max)
    
    times_bnds = xr.DataArray(np.stack([times_bnds_min, times_bnds_max], axis=1), dims = ["time","bnds"], coords = {"time":times_stmp})
    
    
    seasonal_cycle = xr.DataArray(np.sin(2*np.pi*(times_stmp.dayofyear/365.25 - 0.25)), dims = ["time"], coords = {"time":times_stmp})
    
    
    return xr.merge([seasonal_cycle.rename("seasonal_cycle"), times_bnds.rename("time_bnds")])