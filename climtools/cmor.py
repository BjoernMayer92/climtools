
import logging
import os

necessary_cmor_attrs = ["mip_era","activity_id","institution_id", "source_id", "experiment_id","table_id","variable_id","grid_label","version_id" ]
necessary_cmor_coords = ["member_id"]


def check_neccessary_cmor(data):
    """Checks if all the neccessary attributes and coordinates exists for saving in cmor format

    Args:
        data (xarray.DataArray or xarray.Dataset): Dataset investigated
    """
    non_existent_attrs = []
    for key in necessary_cmor_attrs:
        if not (key in data.attrs):
            non_existent_attrs.append(key)

    non_existent_coords = []
    for key in necessary_cmor_coords:
        if not (key in data.coords):
            non_existent_coords.append(key)

    assert len(non_existent_attrs) == 0, "Attributes {} missing in data".format(non_existent_attrs)
    assert len(non_existent_coords)== 0, "Coordinates {} missing in data".format(non_existent_coords)
    
def gen_cmor_path_and_filename(data):
    """Generates from the attributes a cmor path and a cmor filename

    Args:
        data (xarray.DataArray or xarray.Dataset): xarray object for which the path and filename should be generated
    """
    check_neccessary_cmor(data)
    
    mip_era = data.mip_era
    activity_id = data.activity_id
    institution_id = data.institution_id
    source_id = data.source_id
    experiment_id = data.experiment_id
    member_id = data.member_id.item()
    if "process_id" in data.attrs:
        table_id = data.attrs["process_id"]
    else:
        table_id = data.table_id
    table_id = data.table_id
    variable_id = data.variable_id
    grid_label = data.grid_label
    version_id = data.version_id

    cmor_path = os.path.join(mip_era, activity_id, institution_id, source_id, experiment_id, member_id, table_id, variable_id, grid_label, version_id)
    cmor_file = "_".join([variable_id, table_id, source_id, experiment_id, member_id, grid_label])+".nc"
    
    logging.info("Cmor Path: {}".format(cmor_path))
    logging.info("Cmoe File: {}".format(cmor_file))
    return cmor_path, cmor_file


def cmor_save(data, parent_directory):
    """ Save a dataarray or dataset in conform with cmor regulations under the parent directory

    Args:
        data (xarray.DataArray or xarray.Dataset): Data to be saved
        parent_directory (string): Path to the parent folder where the CMIP-folder structure starts 
    """

    cmor_path, cmor_file = gen_cmor_path_and_filename(data)
    comp_path = os.path.join(parent_directory, cmor_path)
    
    os.makedirs(comp_path, exist_ok=True)
    data.to_netcdf(os.path.join(comp_path, cmor_file))

    
def update_process_id(data, process_string):
    """Updates or assings a new attribute "process_id" to the dataset in order to track changes

    Args:
        data (xarray.DataArray or xarray.Dataset): Data where the attribute should be updated
        process_string (string): String that indicates the update that has been done on the data 

    Returns:
        [type]: [description]
    """
    if "process_id" not in data.attrs:
        data = data.assign_attrs({"process_id":process_string})
    else:
        previous_string = data.attrs["process_id"]
        updated_string = "_".join([previous_string, process_string])
        data = data.assign_attrs({"process_id":updated_string})
    return data