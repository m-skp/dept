# add parent repository path to find dept
import sys; sys.path.append('..')
from dept.base import *
from dept.modules.aws import *


if __name__ == "__main__":

    s3_connection_config = read_file(f"{DEPT_PATH}/configs/aws.json")
    file_details = s3_scan_repository(
        s3_connection_config = s3_connection_config,
        s3_bucket=s3_connection_config.get('bucket_name')
    )