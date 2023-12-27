import os
import json
import pandas as pd
from datetime import datetime

# print current location
print(f"current location: {os.getcwd()}")


#############################################################################
# FILE SYSTEM
#############################################################################

def norm_path(
    str_path: str=None
    ):
    """
    - normalizes file paths to \\ usage
    """
    return str_path.replace('/','\\')


#############################################################################

def read_file(
    file_path: str=None,
    json_to_dict: bool=True
    ):
    """
    - reads a flat file into a string
        -> file_path - path to the target file
        -> json_to_dict - reads .json file types as dictionaries
    """
    
    with open(file_path) as f:
        if os.path.basename(file_path).endswith('json') and json_to_dict==True:
            content = json.load(f)
        else:
            content = f.read()

    return content


#############################################################################

def write_file(
    content,
    file_path: str=None,
    auto_json: bool=True,
    encoding: str='utf-8'
    ):
    """
    - writes a file
        -> file_path - path to the target file
        -> auto_json - if file_path ends with .json, automatically handle escape characters 
    """

    with open(file_path, 'w', encoding=encoding) as f:

        if os.path.basename(file_path).endswith('json') and auto_json==True:
            json.dump(content, f, ensure_ascii=False, indent=2)
        else:
            f.write()

    return content


#############################################################################
# MISCELLANEOUS
#############################################################################

def print_dict(
    d: dict=None,
    indent: int=2,
    sort_keys: bool=False
    ):
    """
    - prints out dictionary with indented keys
    """

    print(json.dumps(d, indent=indent, sort_keys=sort_keys))


#############################################################################
#############################################################################


if __name__ == "__main__":

    print(os.getcwd())
