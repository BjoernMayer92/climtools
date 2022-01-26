import xarray as xr


def decompose_dependent_variables(data, dimension):
    """Returns all variables of a dataset that depend on a specified dimension

    Args:
        data (xarray.Dataset): Dataset to be analysed
        dimension (string): Name of the dimension

    Returns:
        dict: dictionary containing the names of dependent and independent variables 
    """

    dependent_variables = []
    independent_variables = []
    
    for variable in data.data_vars:
        dimensions = data.data_vars[variable].dims
        if dimension in dimensions:
            dependent_variables.append(variable)
        else:
            independent_variables.append(variable)
            
    return {"dependent": dependent_variables, "independent": independent_variables}
