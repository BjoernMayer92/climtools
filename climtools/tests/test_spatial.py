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

    def test_sellonlatbox(self):
        return

    def test_gen_lonlatbox_max(self):
        return

    def test_transformation(self, data, lon_dim="lon"):
        return 
