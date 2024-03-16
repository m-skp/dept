from dept.base import *
import boto3

#############################################################################
# AWS 
#############################################################################

def _aws_connection(
    aws_connection_config: dict=None,
    service_name: str=None,
    concept: str=None
    ) -> object:
    """
    establishes connection to S3

    Parameters
    ----------
    aws_connection_config : dict
        AWS connection configuration, by default None
    service_name : str
        AWS service name, by default None
    concept : str
        AWS concept, by default None
        -> ['resource','client','waiter,'paginator']

    Returns
    -------
    bool
        True 
    """       

    # create session
    aws_connection_config = drop_none_value_keys(aws_connection_config)

    if aws_connection_config is not None:
    # read credentials from config file if provided
        
        session = boto3.session.Session(
            aws_access_key_id = aws_connection_config.get('access_key'),
            aws_secret_access_key = aws_connection_config.get('secret'),
            region_name = aws_connection_config.get('region_name'),
        )


    else:
    # use system default connection configuration
        
        session = boto3.session.Session()

    # define endpoint_url argument if provided
    endpoint_url = aws_connection_config.get('endpoint_url')
    if endpoint_url is not None: 
        endpoint_string = f"endpoint_url={endpoint_url}"
    else: 
        endpoint_string = ''

    # define connection string
    connection_string = f"session.{concept}('{service_name}', {endpoint_string})"
    
    return eval(connection_string)


#############################################################################
# S3 
#############################################################################

@decorator_timer
def s3_collect_file(
    s3_connection_config: dict=None,
    s3_bucket: str=None,
    s3_file_path: str=None,
    local_destination_path: str=None
    ) -> bool:
    """
    collects file from S3

    Parameters
    ----------
    s3_connection_config : dict
        S3 connection configuration, by default None
    s3_bucket : str, optional
        S3 bucket name, if not provided access_key is used, by default None
    s3_file_path : str, optional
        file location within the S3 bucket, by default None
    destination_file_path : str
        local destination path for the collected file, by default None

    Returns
    -------
    bool
        True 
    """

    try:
        
        # establish S3 client connection
        s3 = _aws_connection(s3_connection_config, 's3', 'client')

        # download data
        s3.download_file(s3_bucket, s3_file_path, local_destination_path)

        return True
    
    except Exception as e:
        print(f"failed to download from S3: {s3_bucket}/{s3_file_path} to {local_destination_path}")
        print(e)
        raise ValueError


#############################################################################
    
@decorator_timer
def s3_upload_file(
    s3_connection_config: dict=None,
    s3_bucket: str=None,
    source_file_path: str=None,
    s3_destination_path: str=None
    ) -> bool:
    """
    uploads file to S3

    Parameters
    ----------
    s3_connection_config : dict
        S3 connection configuration, by default None
    s3_bucket : str, optional
        S3 bucket name, if not provided access_key is used, by default None
    source_file_path : str
        local file path to the file for upload, by default None
    s3_destination_path : str
        S3 destination for the uploaded file, by default None

    Returns
    -------
    bool
        True 
    """

    try:

        # establish S3 client connection
        s3 = _aws_connection(s3_connection_config, 's3', 'client')

        # upload object
        with open(source_file_path, "rb") as f:
            s3.upload_fileobj(f, s3_bucket, s3_destination_path)

        return True
    
    except Exception as e:
        print(f"failed to upload {source_file_path} to S3: {s3_bucket}/{s3_destination_path}")
        print(e)
        raise ValueError


#############################################################################
    
@decorator_timer
def s3_scan_repository(
    s3_connection_config: dict=None,
    s3_bucket: str=None,
    s3_path: str=None,
    file_types: list=None,
    regex_pattern: str=None
    ) -> bool:
    """
    scans S3 repository for files of specified file_types following a regex_pattern

    Parameters
    ----------
    s3_connection_config : dict
        S3 connection configuration, by default None
    s3_bucket : str, optional
        S3 bucket name, if not provided access_key is used, by default None
    s3_path : str, optional
        S3 path to be scanned, by default None
    file_types : list, optional
        list of file types to pick, by default None
    regex_pattern : str
        file name regex pattern to use, by default None

    Returns
    -------
    bool
        True 
    """

    # set default values
    s3_path = s3_path or ''
    regex_pattern = regex_pattern or '.*'
    file_types = file_types or ['']
    s3_object_list = []

    try:

        # establish S3 client connection
        s3 = _aws_connection(s3_connection_config, 's3', 'client')

        # get list of objects
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_path)

    except Exception as e:
        print(f"unable to read S3 repository {s3_bucket}/{s3_path}")
        print(e)
        raise ValueError

    # iterate over pages and objects
    for page in pages:
        for obj in page['Contents']:

            # check if object ends with any of the suffixes
            if obj['Key'].endswith(tuple(file_types)) and re.search(regex_pattern, obj['Key']):
                s3_object_list.append(obj)

    return s3_object_list


#############################################################################
#############################################################################



