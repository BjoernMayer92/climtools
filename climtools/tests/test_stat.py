import unittest
import os
import sys
import xarray as xr
import cftime

from climtools import temporal 
from climtools import stat
from helper_functions import *

class TestStat(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        super(TestStat, self).__init__(*args, **kwargs)
        self.data_directory = os.path.join(os.getcwd(),"data")
        if not os.path.exists(self.data_directory):
            os.mkdir(self.data_directory)
        
        self.start = cftime.datetime(1850,1,1,0,0,0, calendar="proleptic_gregorian")
        self.end   = cftime.datetime(2100,1,1,0,0,0, calendar="proleptic_gregorian")

    
    def test_create_mono_timeseries(self):
        self.data_1d = stat.gen_test_mono_timeseries(self.start, self.end)
        time = self.data_1d.time

        time_values = time.dt.day + time.dt.month*100 + time.dt.year*10000
        xr.testing.assert_equal(self.data_1d["values"], time_values)


if __name__ == '__main__':
    unittest.main()
