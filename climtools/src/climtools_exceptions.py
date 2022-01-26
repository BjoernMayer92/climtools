"""

Defines exceptions for the climtool package

"""


class DimensionError(Exception):
    """
    Raised when neccessary dimension doesnt exist in an xarray object
    """
    
    def __init__(self, dim, dim_list):
        message = "Dimension " + str(dim) + " is not in list " +str(dim_list)
        super().__init__(message)
    
