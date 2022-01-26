import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import warnings
import logging
from . import utils

temporal_resolution_dict = dict(
day="1D",
month="1MS",
season = "QS",
season_dec = "QS-DEC",
season_jan = "QS-JAN",
season_feb = "QS-FEB",
year ="1Y")

timedelta_1D = np.timedelta64(1,"D")


def cal_timedelta_year(time):
    """[summary]

    Args:
        time ([type]): [description]

    Returns:
        [type]: [description]
    """
    timedelta = (365 + time.dt.is_leap_year)*timedelta_1D
    return timedelta
    

def cal_timedelta_month(time):
    """[summary]

    Args:
        time ([type]): [description]

    Returns:
        [type]: [description]
    """
    timedelta = time.dt.daysinmonth*timedelta_1D
    return timedelta


def get_temporal_resolution(data):
    """ Returns the temporal resolution of a given xarray object from the time_bnds variable
    Args:
        data (Dataset): Input Dataset must contain at least the dimension "time_bnds"

    Returns:
        str: Name of the temporal resolution detected
    """

    time_stmp = data.time
    time_bnds = data.time_bnds
    timedelta = time_bnds.isel(bnds=1)-time_bnds.isel(bnds=0)
    
    data.data_vars

    
    if (timedelta == timedelta_1D).all():
        return "day"

    timedelta_1M = cal_timedelta_month(time_stmp)
    if(timedelta == timedelta_1M).all():
        return "month"
    
    timedelta_1Y = cal_timedelta_year(time_stmp)
    if(timedelta == timedelta_1Y).all():
        return "year"
        


def cal_middle_time(time_1, time_2):
    """[summary]

    Args:
        time_1 ([type]): [description]
        time_2 ([type]): [description]

    Returns:
        [type]: [description]
    """
    timedelta = (time_2 - time_1)/2
    time = time_1 + timedelta
    
    return time
    

def gen_time_bnds_stmp(resample):
    """[summary]

    Args:
        resample ([type]): [description]

    Returns:
        [type]: [description]
    """
    time_bnds_min = resample.first().time_bnds.isel(bnds=0)
    time_bnds_max = resample.last().time_bnds.isel(bnds=1)
    
    time_bnds_min = xr.CFTimeIndex(time_bnds_min.values)
    time_bnds_max = xr.CFTimeIndex(time_bnds_max.values)

    time_stmp = cal_middle_time(time_bnds_min, time_bnds_max)

    time_bnds_min = xr.DataArray(time_bnds_min, dims = ["time"], coords = {"time":time_stmp}) 
    time_bnds_max = xr.DataArray(time_bnds_max, dims = ["time"], coords = {"time":time_stmp})
    
    time_bnds = xr.concat([time_bnds_min, time_bnds_max], dim ="bnds")
    return time_bnds.rename("time_bnds")






    
def temporal_downsampling(data, target_resolution):
    """
    
    
    """
    variables = [variable_name for variable_name in data.data_vars]
    temporal_resolution = get_temporal_resolution(data)
    
    assert "time_bnds" in variables, "Dataset must have at least the dimension time_bnds"
    assert list(temporal_resolution_dict).index(temporal_resolution) < list(temporal_resolution_dict).index(target_resolution), "Target resolution must be coarser than the temporal resoltion of the dataset"
    
    decomposition_dependent_variables = utils.decompose_dependent_variables(data, dimension="time")
    
    resample_variables = decomposition_dependent_variables["dependent"]
    leftover_variables = decomposition_dependent_variables["independent"]
    
    
    
    if temporal_resolution =="month":
        weights = get_monthly_weights(data, target_resolution)
        data_weighted = data.copy()
        for variable in resample_variables:
            if variable!="time_bnds":
                data_weighted[variable] = data[variable]*weights
                
        
        data_resample = data_weighted[resample_variables].resample(time = temporal_resolution_dict[target_resolution])
        data = data_resample.sum()
        
    else:
        data_resample = data[resample_variables].resample(time = temporal_resolution_dict[target_resolution])
        
        data = data_resample.mean()

    time_bnds = gen_time_bnds_stmp(data_resample)
    data = data.assign_coords(time = time_bnds.time)
    #return data, leftover_variables, time_bnds

    data = xr.merge([data, time_bnds, data[leftover_variables]])

    return data
    
    