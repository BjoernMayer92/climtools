import numpy 
import math
import cftime

def gaussian_formula(number):
    """[summary]

    Returns:
        [type]: [description]
    """
    return 0.5*number*(number+1)

def sum_interval(start, end):
    """[summary]

    Args:
        start ([type]): [description]
        end ([type]): [description]

    Returns:
        [type]: [description]
    """
    return gaussian_formula(end) - gaussian_formula(start)


def try_is_leap(year, calendar):
    try:
        cftime.datetime(year=year,month=2,day=29,second=0, calendar=calendar)
    except:
        return False
    else:
        return True