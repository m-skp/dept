import os
import re
import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone
import uuid
import hashlib

# print current location
print(f"current location: {os.getcwd()}")

#############################################################################
# VARIABLES
#############################################################################

DEPT_PATH = os.path.dirname(os.path.realpath(__file__))
TIMER_FORMAT = '%Y-%m-%d %H:%M:%S'

#############################################################################
# DECORATORS
#############################################################################

def decorator_timer(func):
    """
    function runtime timer
    """
    def wrapper(*args, **kwargs):
        
        # start timer
        f_start = datetime.now()
        print(f"{f_start.strftime(TIMER_FORMAT)} - {func.__name__} started")

        # run function
        x = func(*args, **kwargs)

        # end timer
        f_end = datetime.now()
        print(f"{f_start.strftime(TIMER_FORMAT)} - {func.__name__} completed in {f_end - f_start} s")

        return x
    
    return wrapper


#############################################################################
# FILE SYSTEM
#############################################################################

def norm_path(
    str_path: str=None
    ) -> str:
    """
    normalizes file paths to \\ usage

    Parameters
    ----------
    str_path : str
        path to normalize, by default None

    Returns
    -------
    str
        normalized path string
    """

    try:
        n_path = os.path.join(*re.split(r'\\|/', str_path))
        return str_path.replace('/','\\')
    
    except Exception as e:
        print(f'ERROR: unable to normalize "{str_path}"')
        print(e)
        return None


#############################################################################

def read_file(
    file_path: str=None,
    json_to_dict: bool=True
    ) -> str:
    """
    reads a flat file into a string

    Parameters
    ----------
    file_path : str
        path to the target file, by default None
    json_to_dict : bool, optional
        reads .json file types as dictionaries, by default True

    Returns
    -------
    str
        content string
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
    ) -> bool:
    """ 
    writes content into a file

    Parameters
    ----------
    content 
        file content
    file_path : str
        path to the target file, by default None
    auto_json : bool, optional
        if file_path ends with .json, automatically handle escape characters, by default True
    encoding: str, optional
        file encoding to use, by default 'utf-8'    
    
    Returns
    -------
    bool
        True upon success
    """

    with open(file_path, 'w', encoding=encoding) as f:

        if os.path.basename(file_path).endswith('json') and auto_json==True:
            json.dump(content, f, ensure_ascii=False, indent=2)
        else:
            f.write(content)

    return True


#############################################################################

def scan_files(
    repository_address: str=None,
    file_types: list=None,
    regex_pattern: str=None
    ) -> list:
    """
    recursive scans repository_address for files of specified file_types following a regex_pattern

    Parameters
    ----------
    repository_address : str
        path to repository to scan, by default None
    file_types : list, optional
        list of file types to pick, by default None
    regex_pattern : str, optional
        file name pattern, by default None

    Returns
    -------
    list
        list of file path strings
    """

    # set default values
    file_types = file_types or ['']
    file_list = []

    # scan repository
    for root, dirs, files in os.walk(repository_address):
        for name in files:
            if name.endswith(tuple(file_types)) and re.search(regex_pattern, name):
                file_path = os.path.join(root, name)
                file_list.append(norm_path(file_path))

    return file_list


#############################################################################
# MISCELLANEOUS
#############################################################################

def print_dict(
    d: dict=None,
    indent: int=2,
    sort_keys: bool=False
    ):
    """
    prints out dictionary with indented keys

    Parameters
    ----------
    d : dict
        input dictionary, by default None
    indent : int, optional
        indentation, by default 2
    sort_keys : bool, optional
        sort JSON keys, by default False
    """

    print(json.dumps(d, indent=indent, sort_keys=sort_keys, default=str))


#############################################################################
    
def md5_hash(
    tokens: list=None,
    separator: str='|',
    null_replacement: str='null',
    null_values: list=['',None,np.NaN],
    case_sensitivity: bool=False
    ) -> str:
    """
    _summary_

    Parameters
    ----------
    tokens : list
        list of values to hash, by default None
    separator : str, optional
        ccharacter to use as a token separator, by default '|'
    null_replacement : str, optional
        value to populate missing tokens, by default 'null'
    null_values : list, optional
        list of values identifying missing tokens, by default ['',None,np.NaN]
    case_sensitivity : bool, optional
        consider case sensitivity, by default False

    Returns
    -------
    str
        32-char md5 hash of the token list
    """

    # map tokens to string
    tokens_text = map(str, tokens)

    # populate missing values
    tokens_cleaned = separator.join([null_replacement if i in null_values else i for i in tokens_text])
    
    # convert to lowercase if case sensitivity is not applied
    if case_sensitivity == False: tokens_cleaned = tokens_cleaned.lower()

    return hashlib.md5(tokens_cleaned.encode('utf-8')).hexdigest()


#############################################################################

def generate_id() -> str:
    """
    function to generate a uniqe 32-char ID

    Returns
    -------
    str
        32-char unique ID
    """

    uid = str(uuid.uuid4())
    timestamp = datetime.now().astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
    user = os.getlogin()

    return md5_hash([uid, user, timestamp])


#############################################################################

def normalize_key(
    s: str=None
    ) -> str:
    """
    function to normalize free text into a normalized key form
        - replaces special characters with _
        - removes multiplicated _
        - converts to lowercase
        - removes leading and tailing whitespaces

    Parameters
    ----------
    s : str
        input string, by default None

    Returns
    -------
    str
        normalized key string
    """

    return re.sub('\W{1,}', '_', s.lower().strip(' _'), flags=re.IGNORECASE)


#############################################################################

def json_drop_null_value_keys(
    json_string: str=None,
    null_values: list=["", "null"]
    ) -> str:
    """
    removes null-value JSON keys

    Parameters
    ----------
    json_string : str
        valid JSON string, by default None
    null_values : list
        list of strings to consider as null in addition to proper null, by default ["", "null"]

    Returns
    -------
    str
        JSON string without null-value keys 
    """

    # prepare null value regex
    null_values_regex = '(null\s*,*)' + ''.join([f'|({r}\s*,*)' for r in null_values])

    # remove null values
    cleaned_json_string = re.sub('(\"([^\"]*)\"\s*:\s*(' + null_values_regex + '))', '', json_string, flags=re.IGNORECASE)
    
    # remove last comma if present to get correct JSON structure
    out_json_string = re.sub(',*\s*\}*\s$', '}', cleaned_json_string) 

    return out_json_string

        

#############################################################################

def drop_none_value_keys(
    d: dict=None
    ) -> dict:
    """
    removes dictionary keys containing None value

    Parameters
    ----------
    d : dict
        dictionary, by default None

    Returns
    -------
    dict
        dictionary without None values
    """

    if isinstance(d, dict):
        return type(d) ((drop_none_value_keys(k), drop_none_value_keys(v)) for k,v in d.items() if k is not None and v is not None)
    else:
        return d


#############################################################################
    
def consolidate_configs(
    input_config: dict={},
    default_config: dict={},
    path: list=[]
    ) -> dict:
    """
    supplies missing keys in input_config from default_config

    Parameters
    ----------
    input_config : dict
        input configuration dictionary, by default {}
    default_config : dict
        default configuration dictionary, by default {}
    path : list
        path tracking for nested dictionaries, by default []

    Returns
    -------
    dict
        consolidated config dictionary
    """

    for key in default_config:
        if key in input_config:
            if isinstance(input_config[key], dict) and isinstance(default_config[key], dict):
                consolidate_configs(input_config[key], default_config[key], path + [str(key)])
            else:
                pass
        else:
            input_config[key] = default_config[key]

    return input_config
        



#############################################################################
#############################################################################


if __name__ == "__main__":

    # print location
    print(os.getcwd())

    run_id = generate_id()
    print(f"test run: {run_id}")

