import xarray as xr
import logging
import datetime
xr.set_options(keep_attrs = True)


def decompose_dependent_variables(data, dimensions):
    """Returns all variables of a dataset that depend on a specified dimension

    Args:
        data (xarray.Dataset): Dataset to be analysed
        dimension (tuple): Name of the dimension

    Returns:
        dict: dictionary containing the names of dependent and independent variables 
    """

    dependent_variables = []
    independent_variables = []
    
    for variable in data.data_vars:
        data_dimensions = data.data_vars[variable].dims
        if set(dimensions).issubset(set(data_dimensions)):
            dependent_variables.append(variable)
        else:
            independent_variables.append(variable)
            
    logging.info("Dependent variables {} Independent Variables {}".format(dependent_variables, independent_variables))
        
    return {"dependent": dependent_variables, "independent": independent_variables}


def cal_weighting_factor(data, dimensions):
    """
    Args:
        data ([type]): [description]
        dimension ([type]): [description]
    """

    weights = data/data.sum(dim=dimensions)
    weights_sum = weights.sum(dim=dimensions)
    xr.testing.assert_allclose(weights_sum , xr.ones_like(weights_sum))
    return weights    

def add_history(data, processing_message):
    """Appends  a new message with timestamp to the history attribute of a dataset

    Args:
        data (xarray.Dataset or xarray.DataArray): dataset or dataarray for which history is to be changed
        processing_message (str): Message describing the processing step
    """
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    message_timestamped = " {} ; {} ".format(timestamp, processing_message)
    
    if "history" in data.attrs.keys():
        data.attrs["history"] = data.attrs["history"] + message_timestamped
    else:
        data.assign_attrs({"history":message_timestamped})

def add_table_id(data, processing_id):
    """_summary_

    Args:
        data (xarray,Dataset or xarray.DataArray): Dataset or array for which the table_id needs to be changed
        processing_id (str): ID indicative of the processing step 
    """
    if "table_id" in data.attrs.keys():
        data.attrs["table_id"] = "_".join([data.attrs["table_id"], processing_id])
    else:
        data.assign_attrs({"table_id": processing_id})

def add_processing_attributes(data, processing_message, processing_id):
    """Changes history and table_id attributes

    Args:
        data (xarray.Dataset or xarray.DataArray): 
        processing_message (string): Message describing the processing step
        processing_id (string): ID indicative of the processing step
    """

    add_table_id(data, processing_id)
    add_history(data, processing_message)


def pre_attrs_to_dims(ds,attribute_keys):
    """Adds attributes of a given Dataset as dimensions

    Args:
        ds (xarray.Dataset/xarray.DataArray): 
        attribute_keys (list of str):  

    Returns:
        xarray.Dataset or xarray.DataArray: _description_
    """
    for key in attribute_keys:
        ds = ds.assign_coords({key:ds.attrs[key]})
        ds = ds.expand_dims(key)

    return ds


def func_attrs_to_dims(attribute_keys):
    """_summary_

    Args:
        attribute_keys (_type_): _description_

    Returns:
        _type_: _description_
    """
    return lambda ds: pre_attrs_to_dims(ds, attribute_keys)