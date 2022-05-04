import xarray as xr
import numpy as np
import os
import pickle

def xarray_save_multiindex(data, folder):
    os.makedirs(folder, exist_ok=True)

    dict_coords = {}
    for dim in data.dims:
        coords = data[dim].coords.to_dataset()[dim]
        dict_coords[dim] = coords
    
    np.save(os.path.join(folder, "data.npy"), data.values)
    with open(os.path.join(folder,'coordinates.pickle'), 'wb') as handle:
        pickle.dump(dict_coords, handle, protocol=pickle.HIGHEST_PROTOCOL)


def xarray_load_multiindex(folder):
    dict_coords = load_coordinates(folder)
    data = np.load(os.path.join(folder,"data.npy"))
    
    data = xr.DataArray(data, dims = list(dict_coords.keys()), coords = dict_coords)

    return data

def load_coordinates(folder):
    with open(os.path.join(folder,'coordinates.pickle'), 'rb') as handle:
        dict_coords = pickle.load(handle)
    return dict_coords

