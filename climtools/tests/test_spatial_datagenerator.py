from climtools import stat
from climtools import spatial_datagenerator
import matplotlib.pyplot as plt
import xarray as xr
import unittest
import numpy as np

class TestStat(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super(TestStat, self).__init__(*args, **kwargs)
        self.n_sample = 1000000
        self.lonlat_resolution = 30
        self.distance_decay_constant = 40000
        self.data = spatial_datagenerator.xarray_spatial_correlated_distance_regular_grid(n_sample = self.n_sample,
                                                                                          lonlat_resolution = self.lonlat_resolution,
                                                                                          distance_decay_constant = self.distance_decay_constant)

    def test_xarray_spatial_correlated_distance_regular_grid(self):
        data_mean = self.data["data"].mean(dim="sample")
        
        xr.testing.assert_allclose(data_mean, xr.zeros_like(data_mean), atol = 1/np.sqrt(self.n_sample)*3 )

    def test_xarray_autocovariance_matrix(self):
        covariance_ori = self.data["covariance"].rename({"lon_2":"lon_1", "lat_2":"lat_1"})
        covariance_cal = spatial_datagenerator.xarray_autocovariance_matrix(self.data["data"], sample_dim="sample")
        
        xr.testing.assert_allclose(covariance_ori, covariance_cal, atol = 1/np.sqrt(self.n_sample)*3)

if __name__ == '__main__':
    unittest.main()