"""
Defines the xarray accessor for xarray containing preprocessing functions

"""

import climtools_exceptions
import numpy as np




def anomaly(data, dims):
    """
    Calculates the anomaly with respect to the specified dimensions

    Parameters
    ----------
    dims: dictionary of dimensions with respect to which anomalies are calculated
    Return
    ------
    data_anom: xarray dataarray or dataset
        Calculated Anomalies
    """
    
    return data-data.mean(dim=dims)




######################
# Define Accessor
######################

@xr.register_dataarray_accessor("preproc")

class preproc:
    def __init__(self, xarray_obj):
        self._obj = xarray_obj

        
    def time_anomaly(self, dim, timeframe):
        """
        Calculates anomalies with respect to a given timeframe
        
        
        """    
        #TODO Check for dtypes of dimensions, but cftime makes problems
        
        grouped = self._obj.groupby(dim+"."+timeframe)
        
        
        return anomaly(grouped, dims=dim)
    
    
    def anomaly(self, dims={"time"}):
        """
        Calculates the anomaly with respect to the specified dimensions

        Parameters
        ----------
        dims: dictionary of dimensions with respect to which anomalies are calculated
        
        
        
        Returns
        ------
        data_anom: xarray dataarray or dataset
            
        """
        
        return anomaly(self._obj, dims= dims)
    
    