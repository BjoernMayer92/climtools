import xarray as xr
import logging
import datetime as datetime
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
        processing_message (str): Message string
    """
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    message_timestamped = " {} ; {} ".format(timestamp, processing_message)
    data.attrs["history"] = data.attrs["history"] + message_timestamped
    
def add_table_id(data, processing_id):
    """_summary_

    Args:
        data (_type_): _description_
        processing_id (_type_): _description_
    """

    data.attrs["table_id"] = "_".join([data.attrs["table_id"], processing_id])

def add_processing_attributes(data, processing_message, processing_id):
    """Changes history and table_id attributes

    Args:
        data (xarray.Dataset or xarray.DataArray): 
        processing_message (_type_): _description_
        processing_id (_type_): _description_
    """

    add_table_id(data, processing_id)
    add_history(data,processing_message)
