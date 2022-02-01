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
    
    times_bnds = generate_timeseries(start, end, frequency)
    
    seasonal_cycle = xr.DataArray(np.sin(2*np.pi*(times_bnds.time.dt.dayofyear/365.25 - 0.25)), dims = ["time"], coords = {"time":times_bnds.time})
    
    
    return xr.merge([seasonal_cycle.rename("seasonal_cycle"), times_bnds.rename("time_bnds")])


def generate_timeseries(start, end, frequency):
    """[summary]

    Args:
        start ([type]): [description]
        end ([type]): [description]
        frequency ([type]): [description]

    Returns:
        [type]: [description]
    """
    assert frequency in temporal.temporal_resolution_dict, "Frequency must be one the following: "+str(list(temporal.temporal_resolution_dict.keys()))

    frequency_str = temporal.temporal_resolution_dict[frequency]

    times_bnds_min = xr.cftime_range(start=start, end=end,  freq=frequency_str, calendar="proleptic_gregorian")[:-1]
    times_bnds_min = times_bnds_min.shift(1,"0D")
    times_bnds_max = times_bnds_min.shift(1,frequency_str)

    times_stmp = temporal.cal_middle_time(times_bnds_min, times_bnds_max)

    times_bnds = xr.DataArray(np.stack([times_bnds_min, times_bnds_max], axis=1), dims = ["time","bnds"], coords = {"time":times_stmp})
    return times_bnds

def gen_test_mono_timeseries(start, end):
    """Generates an artifical seasonal cycle for times between a given start and endpoint with a given frequncy

    Args:
        start (cftime.Datetime): Beginning of the period
        end (cftime.Datetime): End of the period
        frequency (string): 

    Returns:
        xarray.Dataset: Dataset containing seasonal cycle
    """
    
    times_bnds = generate_timeseries(start, end, "day")
    times = times_bnds.time
    data = times.dt.day + times.dt.month*100 + times.dt.year*10000

    data = xr.DataArray(data.values, dims = ["time"], coords = {"time":times_bnds.time})
    
    
    return xr.merge([data.rename("values"), times_bnds.rename("time_bnds")])