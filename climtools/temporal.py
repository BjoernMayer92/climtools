import xarray as xr
xr.set_options(keep_attrs = True)
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
    """Calculates the duration for each year in each timestamp

    Args:
        time (xarray.DataArray): DataArray containing cftime.Datetime Objects

    Returns:
        [:obj: `np.array` of :obj: `timedelta64`]: [description]
    """
    leap_years = [is_leap_year(time_value) for time_value in time.values]

    timedelta = (365 + np.array(leap_years))*timedelta_1D
    return timedelta
    

def cal_timedelta_month(time):
    """Calculates the duration for each month in each timestamp

    Args:
        time (xarray.DataArray): DataArray containing cftime.Datetime Objects

    Returns:
        [:obj: `np.array` of :obj: `timedelta64`]: [description]
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

    time_stmp = data.time.compute()
    time_bnds = data.time_bnds.compute()
    time_max = time_bnds.isel(bnds=1)
    time_min = time_bnds.isel(bnds=0)
    timedelta = xr.apply_ufunc(np.subtract,time_max, time_min, dask="parallelized")
    
    if (timedelta == timedelta_1D).all():
        logging.info("Timedelta day identified")
        return "day"

    timedelta_1M = cal_timedelta_month(time_stmp)
    if(timedelta == timedelta_1M).all():
        logging.info("Timedelta month identified")
        return "month"
    
    timedelta_1Y = cal_timedelta_year(time_stmp)
    if(timedelta == timedelta_1Y).all():
        logging.info("Timedelta year identified")
        return "year"

    if(timedelta > timedelta_1D*365).all():
        logging.info("Timdelta multiyear identified")
        return "multiyear"
    else: 
        raise Exception("No valid timedelta could be identified")
        


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
    

def gen_time_bnds_stmp(data, target_resolution):
    """[summary]

    Args:
        resample ([type]): [description]

    Returns:
        [type]: [description]
    """
    resample = data.time_bnds.compute().resample(time = temporal_resolution_dict[target_resolution])

    time_bnds_min = resample.first().isel(bnds=0)
    time_bnds_max = resample.last().isel(bnds=1)
    
    time_bnds_min = xr.CFTimeIndex(time_bnds_min.values)
    time_bnds_max = xr.CFTimeIndex(time_bnds_max.values)

    time_stmp = cal_middle_time(time_bnds_min, time_bnds_max)

    time_bnds_min = xr.DataArray(time_bnds_min, dims = ["time"], coords = {"time":time_stmp}) 
    time_bnds_max = xr.DataArray(time_bnds_max, dims = ["time"], coords = {"time":time_stmp})
    
    time_bnds = xr.concat([time_bnds_min, time_bnds_max], dim ="bnds")
    return time_bnds.rename("time_bnds")

def cal_monthly_weights(data, target_resolution, time_dimension = "time"):
    """[summary]

    Args:
        data ([type]): [description]
        time_dim (str, optional): [description]. Defaults to "time".

    Returns:
        [type]: [description]
    """

    time = data.coords[time_dimension]
    month_length = time.dt.daysinmonth
    
    groupby_string = ".".join([time_dimension,target_resolution])
    
    weights = month_length.groupby(groupby_string) /month_length.groupby(groupby_string).sum(dim=time_dimension)
    
    return weights


def temporal_downsampling(data, target_resolution):
    """This function downsamples (averages) a given dataset to a given target resolution. The target resolutions must be coarser than the time resolution of the dataset provided.py
    
    Args:
        data (xarray.Dataset): Input Dataset. Must contain dimension time as well as the variable time_bnds for inferring the resolution
        target_resolution (string): Target Resolution.

    Returns:
        [xarray.Dataset]: Dataset with a new temporal resolution
    """

    variables = [variable_name for variable_name in data.data_vars]
    temporal_resolution = get_temporal_resolution(data)
    
    assert ("time_bnds" in variables) or ("time_bnds" in data.coords), "Dataset must have at least the dimension or coordinate time_bnds"
    assert target_resolution in temporal_resolution_dict.keys(), "Target Resolutin must be one of the following: {}".format(str(list(temporal_resolution_dict.keys())))
    assert list(temporal_resolution_dict).index(temporal_resolution) < list(temporal_resolution_dict).index(target_resolution), "Target resolution must be coarser than the temporal resoltion of the dataset"
    
    decomposition_dependent_variables = utils.decompose_dependent_variables(data, dimensions=("time",))
    
    resample_variables = decomposition_dependent_variables["dependent"]
    leftover_variables = decomposition_dependent_variables["independent"]
    
    
    
    if temporal_resolution =="month":
        weights = cal_monthly_weights(data, target_resolution)
        data_weighted = data.copy()
        for variable in resample_variables:
            if variable!="time_bnds":
                data_weighted[variable] = data[variable]*weights
                
        
        data_resample = data_weighted[resample_variables].resample(time = temporal_resolution_dict[target_resolution], keep_attrs=True)
        data_result = data_resample.sum(skipna=False)
        
    else:
        data_resample = data[resample_variables].resample(time = temporal_resolution_dict[target_resolution], keep_attrs=True)
        
        data_result = data_resample.mean()

    time_bnds = gen_time_bnds_stmp(data, target_resolution)
    data_result = data_result.assign_coords(time = time_bnds.time)

    data_comb = xr.merge([data_result, time_bnds, data[leftover_variables]], combine_attrs="override")

    utils.add_processing_attributes(data_comb, processing_message="Temporal downsampling from {} to {}".format(temporal_resolution_dict[temporal_resolution],temporal_resolution_dict[target_resolution]) , processing_id=temporal_resolution_dict[target_resolution])


    return data_comb
    

def is_leap_year(time):
    """Checks whether a given cf.Datetime is actually a leap year. Function based on https://github.com/Unidata/cftime/blob/master/src/cftime/_cftime.pyx 

    Args:
        time (cftime.Datetime): Datetime to check
    """

    year = time.year
    calendar = time.calendar
    leap = None

    if calendar == "proleptic_gregorian":
        if year % 4: # not divisible by 4
            leap = False
        elif year % 100: # not divisible by 100
            leap = True
        elif year % 400: # not divisible by 400
            leap = False
        else:
            leap = True
    else:
        warnings.warn("Calendar not implemented yet")
    return leap


def cal_rolling_time_mean(data, time_dim="time", window=10):
    variables_dict = utils.decompose_dependent_variables(data,[time_dim])
    dep_variables = variables_dict["dependent"]
    ind_variables = variables_dict["independent"]

    
    if "time_bnds" in dep_variables:
        dep_variables.remove("time_bnds")

        time_bnds_rol = data["time_bnds"].compute().rolling({time_dim:window}, center=True)
        time_bnds_rol = data["time_bnds"].compute().rolling({time_dim:window}, center=True).construct(window_dim="window_step")
        time_bnds_min = time_bnds_rol.isel(bnds=0, window_step=0)
        time_bnds_max = time_bnds_rol.isel(bnds=1, window_step=-1)

        time_bnds = xr.concat([time_bnds_min, time_bnds_max], dim="bnds")
        data_ind = xr.merge([data[ind_variables], time_bnds.rename("time_bnds")],combine_attrs="override")
    else: 
        data_ind = data[ind_variables]
    
    data_rolling = data[dep_variables].rolling({time_dim:window}, center=True).mean()
    
    processing_id = temporal_resolution_dict[get_temporal_resolution(data)]  +str(window)+"rm"
    processing_message = "Rolling mean over {} time steps applied".format(str(window))
    data_return = xr.merge([data_rolling, data_ind],combine_attrs="override")
    utils.add_processing_attributes(data_return, processing_message, processing_id)

    data_return = data_return.dropna(subset = dep_variables,dim = time_dim, how="all")
    return data_return.dropna(dim="time",how="all")
