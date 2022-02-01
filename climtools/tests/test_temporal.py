import unittest
import os
import sys
import xarray as xr
import cftime
import numpy as np

from climtools import temporal 
from climtools import stat
import helper_functions

class TestStat(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super(TestStat, self).__init__(*args, **kwargs)
        self.data_directory = os.path.join(os.getcwd(),"data")
        if not os.path.exists(self.data_directory):
            os.mkdir(self.data_directory)
        
        self.start = cftime.datetime(1850,1,1,0,0,0, calendar = "proleptic_gregorian")
        self.end = cftime.datetime(2100,1,1,0,0,0, calendar ="proleptic_gregorian")
        self.data_1d = stat.gen_test_mono_timeseries(self.start, self.end)
        self.data_1m = temporal.temporal_downsampling(self.data_1d, "month")
        self.data_1y = temporal.temporal_downsampling(self.data_1d, "year")
        self.data_1y_from_1m = temporal.temporal_downsampling(self.data_1m, "year")

    def test_temporal_downsampling_from_monthly_daily(self):
        xr.testing.assert_allclose(self.data_1y_from_1m, self.data_1y)

    def test_temporal_downsampling_monthly(self):
        time = self.data_1m.time
        days_in_month = time.dt.days_in_month

        monthly_mean = helper_functions.gaussian_formula(days_in_month)/days_in_month

        xr.testing.assert_equal(monthly_mean + time.dt.year*10000 + time.dt.month*100, self.data_1m["values"])

        
    def test_is_leap_year(self):
        start = cftime.datetime(1000,1,1,0,0,0, calendar ="proleptic_gregorian")
        end   = cftime.datetime(3000,1,1,0,0,0, calendar ="proleptic_gregorian")
        time = xr.DataArray(xr.cftime_range(start, end, freq = "1Y"), dims = ["time"])
        
        temporal_leap_year_list = xr.apply_ufunc(temporal.is_leap_year, time, vectorize=True)
        
        try_leap_year_list = [helper_functions.try_is_leap(year, calendar ="proleptic_gregorian") for year in time.dt.year]
        

        np.testing.assert_array_equal(try_leap_year_list, temporal_leap_year_list.values)

    #def test_temporal_downsampling_yearly(self):

        
if __name__ == '__main__':
    unittest.main()
