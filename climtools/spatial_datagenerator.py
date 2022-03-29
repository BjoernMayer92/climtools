import xarray as xr
import numpy as np
earth_radius = 6377.830272


def distance_on_earth(lon_rad_1, lat_rad_1, lon_rad_2, lat_rad_2):
    distance = earth_radius*xr.ufuncs.arccos( xr.ufuncs.sin(lat_rad_1)*xr.ufuncs.sin(lat_rad_2) + xr.ufuncs.cos(lat_rad_1)*xr.ufuncs.cos(lat_rad_2)*xr.ufuncs.cos(lon_rad_2 -lon_rad_1))
    return distance

def xarray_autocovariance_matrix(data, sample_dim="sample"):
    """"
    Args:
        data ([type]): [description]
        sample_dim (str, optional): [description]. Defaults to "time".

    Raises:
        ValueError: [description]

    Returns:
        [type]: [description]
    """
    
    data_dimensions = list(data.dims)

    if not sample_dim in data_dimensions:
        raise ValueError(" ".join(sample_dim, " not in dimensions ", str(data_dimension)))
    else:
        data_dimensions.remove(sample_dim)

    rename_dict = {}
    for data_dimension in data_dimensions:
        rename_dict[data_dimension] = data_dimension+"_1"

    sample_size = data.sizes[sample_dim]
    data_anom_1 = data-data.mean(dim=sample_dim)
    data_anom_2 = data_anom_1.rename(rename_dict)
    
    covariance = xr.dot(data_anom_1, data_anom_2)/(sample_size-1)
    return covariance


def xarray_spatial_correlated_distance_regular_grid(n_sample = 1000, lonlat_resolution=10, distance_decay_constant = 40000):

    # Define Grid
    lat = np.arange(-90, 90, lonlat_resolution)
    lon = np.arange(-180,180, lonlat_resolution)

    n_lat = len(lat)
    n_lon = len(lon)

    # Define Mean State
    mean = xr.DataArray(np.zeros([n_lon, n_lat]),
                        dims = ["lon","lat"],
                        coords = {"lat":lat, "lon":lon})

    mean_stack_1 = mean.rename({"lon":"lon_1", "lat":"lat_1"}).stack(feature_1 = ("lon_1","lat_1"))
    mean_stack_2 = mean.rename({"lon":"lon_2", "lat":"lat_2"}).stack(feature_2 = ("lon_2","lat_2"))

    feature_1 = mean_stack_1.feature_1
    feature_2 = mean_stack_2.feature_2

    lon_rad_1 = xr.ufuncs.deg2rad(feature_1.lon_1)
    lat_rad_1 = xr.ufuncs.deg2rad(feature_1.lat_1)
    lon_rad_2 = xr.ufuncs.deg2rad(feature_2.lon_2)
    lat_rad_2 = xr.ufuncs.deg2rad(feature_2.lat_2)

    distance = distance_on_earth(lon_rad_1, lat_rad_1, lon_rad_2, lat_rad_2)
    
    covariance = xr.ufuncs.exp(-distance/distance_decay_constant)

    result = xarray_multivariate_normal_distribution(mean_stack_1,
                                                     covariance, 
                                                     n_sample=n_sample,
                                                     feature_dim = "feature_1", 
                                                     sample_dim = "sample")
    return xr.merge([result.unstack(dim="feature_1").rename({"lon_1":"lon", "lat_1":"lat"}).rename("data"),
    covariance.unstack().rename({"lon_1":"lon", "lat_1":"lat"}).rename("covariance"), mean.rename("mean")])




def xarray_multivariate_normal_distribution(mean, covariance, n_sample, feature_dim = "location",  sample_dim = "sample"):
    """
    Args:
        mean ([type]): [description]
        covariance ([type]): [description]
        n_sample ([type]): [description]
        feature_dim (str, optional): [description]. Defaults to "location".
        sample_dim (str, optional): [description]. Defaults to "sample".

    Returns:
        [type]: [description]
    """
    data_surrogate = np.random.multivariate_normal(mean, covariance, size = n_sample)
    
    data_surrogate = xr.DataArray(data_surrogate, dims  = [sample_dim, feature_dim], coords = {sample_dim:range(n_sample),
    feature_dim: mean.coords[feature_dim]})

    return data_surrogate