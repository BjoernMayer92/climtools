import xarray as xr
import logging
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

