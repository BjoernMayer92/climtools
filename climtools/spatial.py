import xarray as xr
xr.set_options(keep_attrs = True)
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

    mask = gen_lonlatbox_mask(data, longitude_min, longitude_max, latitude_min, latitude_max, latitude_dim = latitude_dim, longitude_dim = longitude_dim)

    return data.where(mask)

def gen_lonlatbox_mask(data, longitude_min, longitude_max, latitude_min, latitude_max, latitude_dim="lat", longitude_dim ="lon"):
    """ Generates a mask for a longitude and latitude box for a given dataset. Longitude has to be in [0,360]

    Args:
        data (xarray.Dataset): Dataset for which mask is generated
        longitude_min (_type_): Minimum longitude (Minimum not included)
        longitude_max (_type_): Maximum longitude (Maximum not included)
        latitude_min (_type_): Minimum latitude (Minimum not included)
        latitude_max (_type_): Maximum latitude (Maximum not included)
        latitude_dim (str, optional): _description_. Defaults to "lat".
        longitude_dim (str, optional): _description_. Defaults to "lon".

    Returns:
        xarray.DataArray: Mask
    """
    lat = getattr(data,latitude_dim)
    lon = getattr(data,longitude_dim)

    if longitude_min > longitude_max:
        lon_mask_end = (lon>longitude_min) & (lon <= 360)
        lon_mask_sta = (lon>0) & (lon< longitude_max)
        lon_mask = lon_mask_end | lon_mask_sta
    else:
        lon_mask = (lon > longitude_min) & (lon < longitude_max)

    lat_mask = (lat > latitude_min) & (lat<latitude_max)

    mask = lat_mask & lon_mask 
    return mask


def apply_mask(data, mask):
    """Applies a mask to a datast for all variables that have the same dimensions as the mask

    Args:
        data (xarray.Dataset): Dataset which should be masked
        mask (xarray.DataArray): Mask

    Returns:
        xarray.Dataset: Masked data 
    """
    variable_dict = utils.decompose_dependent_variables(data,mask.dims)
    
    dep_variables = variable_dict["dependent"]
    ind_variables = variable_dict["independent"]
    
    data = xr.merge([data[ind_variables],data[dep_variables].where(mask)], combine_attrs="override")
    utils.add_processing_attributes(data, processing_message="Applied mask" , processing_id="masked")
    
    return data 


def cal_weighted_mean(data, weights):
    """_summary_

    Args:
        data (xarray.Dataset): Dataset for which weighted mean should be calculated
        weights (xarray.Dataset): Dataset of weights. Dimension of weights determine the average dimensions

    Returns:
        xarray Dataset: weighted mean over given dimension
    """

    mask_dimensions = weights.dims
    

    variables_dict = utils.decompose_dependent_variables(data, dimensions = mask_dimensions)
    
    dep_variables = variables_dict["dependent"]
    ind_variables = variables_dict["independent"]
    
    dep_variables_weighted_mean = data[dep_variables].weighted(weights.fillna(0)).mean(mask_dimensions)
    
    result = xr.merge([data[ind_variables], dep_variables_weighted_mean], combine_attrs = "override")
    
    utils.add_processing_attributes(result, processing_message = "Calculated weighted mean over dimensions {}".format(str(mask_dimensions)), processing_id="wmean")
    
    return result