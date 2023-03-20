import pandas as pd
import numpy as np
import sys
import os

def get_memory_size(list_of_obj: list):
    return np.sum([sys.getsizeof(x) for x in list_of_obj]) if isinstance(list_of_obj, list) else sys.getsizeof(list_of_obj)

def determine_folder_tree(source_path:str):
    f_dict = {}
    for (dirpath, dirnames, filenames) in os.walk(source_path):
        f_dict[dirpath] = filenames
    return f_dict

def create_dir(path:str=None):
    os.makedirs(path, exist_ok=True)

