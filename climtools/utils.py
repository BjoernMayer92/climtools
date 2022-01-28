import xarray as xr


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
            
    return {"dependent": dependent_variables, "independent": independent_variables}
