import xarray as xr
from . import utils



def sellonlatbox(data, longitude_min, longitude_max, latitude_min, latitude_max, latitude_dim = "lat", longitude_dim = "lon"):
    """Selects points of a given dataset that are in between the boundaries defined by latitude and longitudes mininmums and maximums

    Args:
        data (xarray.Dataset): Input dataset.
        longitude_min (float): Minimum longitude
        longitude_max (float): Maximum longitude
        latitude_min (float): Minimum latitude
        latitude_max (float): Maximum latitude
        latitude_dim (str, optional): Name of the latitude dimension. Defaults to "lat".
        longitude_dim (str, optional): Name of the longitude dimension. Defaults to "lon".

    Returns:
        xarray Dataset: Dataset with masked values
    """
    variables = utils.decompose_dependent_variables(data, getattr(data,latitude_dim).dims)
    dep_variables = variables["dependent"]
    ind_variables = variables["independent"]

    mask = (data.lat < latitude_max) & (data.lat > latitude_min) & (data.lon > longitude_min) & (data.lon < longitude_max) 

    return data.where(mask)