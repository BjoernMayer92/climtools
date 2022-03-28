import xarray as xr
import numpy as np
from . import temporal
from . import utils 

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

def cal_weighted_mean(data, weights):
    """_summary_

    Args:
        data (xarray.Dataset): Dataset for which weighted mean should be calculated
        weights (xarray.DataArray): Dataset of weights. Dimension of weights determine the average dimensions

    Returns:
        xarray Dataset: weighted mean over given dimension
    """

    weights_dimensions = weights.dims
    

    variables_dict = utils.decompose_dependent_variables(data, dimensions = weights_dimensions)
    
    dep_variables = variables_dict["dependent"]
    ind_variables = variables_dict["independent"]
    
    dep_variables_weighted_mean = data[dep_variables].weighted(weights.fillna(0)).mean(weights_dimensions)
    
    result = xr.merge([data[ind_variables], dep_variables_weighted_mean], combine_attrs = "override")
    
    weights_name = weights.name
    dimension_name_string = "-".join(weights_dimensions)
    
    processing_message = "Calculated {} weighted mean over dimensions {}".format(weights_name, dimension_name_string)
    processing_id = "_".join([weights_name, "weightedmean", dimension_name_string])

    utils.add_processing_attributes(result, 
                                    processing_message = processing_message,
                                    processing_id = processing_id)
    
    return result


def cal_anomaly_dim(data, dimensions):
    """Calculates the anomaly over given dimension(s)

    Args:
        data (xarray.Dataset): Dataset for which anomaly is calculated
        dimensions (list[str]): Dimension string or list of dimenstion strings

    Returns:
        xarray.Dataset: Anomaly Dataset
    """
    
    variables_dict = utils.decompose_dependent_variables(data, dimensions = dimensions)
    
    dep_variables = variables_dict["dependent"]
    ind_variables = variables_dict["independent"]

    if "time_bnds" in dep_variables:
        dep_variables.remove("time_bnds")
        ind_variables.append("time_bnds")
        print(dep_variables)
        print(ind_variables)
    
    dep_variables_anom = data[dep_variables] - data[dep_variables].mean(dim=dimensions)
    
    result = xr.merge([data[ind_variables], dep_variables_anom],combine_attrs = "override")
    
    dimension_name_string = "-".join(dimensions)
    
    processing_message = "Calculated {} anomaly".format(dimension_name_string)
    processing_id = "_".join([dimension_name_string,"anomaly"])
    
    utils.add_processing_attributes(result, 
                                    processing_message = processing_message,
                                    processing_id = processing_id)
    return result