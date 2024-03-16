from dept.base import *
import requests
from requests.auth import HTTPBasicAuth
from time import sleep

###########################################################
# CONFIGS
###########################################################

# suppress warnings (e.g. SSL verification issue)
DISABLE_WARNINGS = True
if DISABLE_WARNINGS: 
    from urllib3 import disable_warnings
    disable_warnings()


# DAG ran termination statuses
AIRFLOW_DAG_RUN_TERMINATION_STATUSES = [
    'success', 'failed', 'unreachable'
]
    

###########################################################
# AIRFLOW AUTOMATION
###########################################################

@decorator_timer
def airflow_trigger_dag(
    airflow_connection_config: dict=None,
    airflow_dag_name: str=None,
    airflow_dag_run_config: dict=None       
    ) -> dict:
    """
    function to manually trigger DAG

    Parameters
    ----------
    airflow_connection_config : dict
        Airflow connection configuration, by default None
    airflow_dag_name : str
        DAG name to trigger, by default None
    airflow_config : dict
        DAG run configuration dictionary, by default None

    Returns
    -------
    dict
        Airflow API response
    """    

    # construct API url
    url = f"{airflow_connection_config['url']}/api/v1/dags/{airflow_dag_name}/dagRuns"

    # define Airflow API post json
    api_json = {'conf': airflow_dag_run_config}

    # submit to Airflow API
    r = requests.post(
        url=url,
        json=api_json,
        verify=False,
        auth=HTTPBasicAuth(
            airflow_connection_config['user_name'],
            airflow_connection_config['pwd']
        )
    )

    # read API response
    api_response = json.loads(r.text)

    return api_response


###########################################################

@decorator_timer
def airflow_check_dag_status(
    airflow_connection_config: dict=None,
    airflow_dag_name: str=None,
    airflow_dag_run_id: str=None       
    ) -> dict:
    """
    function to check current run status of a DAG

    Parameters
    ----------
    airflow_connection_config : dict
        Airflow connection configuration, by default None
    airflow_dag_name : str
        DAG name, by default None
    airflow_dag_run_id : str
        DAG run_id, by default None

    Returns
    -------
    dict
        Airflow API response
    """

    # construct API url
    url = f"{airflow_connection_config['url']}/api/v1/dags/{airflow_dag_name}/dagRuns/{airflow_dag_run_id}"

    # submit to Airflow API
    r = requests.get(
        url=url,
        verify=False,
        auth=HTTPBasicAuth(
            airflow_connection_config['user_name'],
            airflow_connection_config['pwd']
        )
    )

    # read API response
    api_response = json.loads(r.text)

    return api_response


###########################################################

def airflow_monitor_dag_run(
    airflow_connection_config: dict=None,
    airflow_dag_name: str=None,
    airflow_dag_run_id: str=None,
    monitoring_interval: int=60,
    timeout_interval_count: int=60
    ) -> dict:
    """
    function to check current run status of a DAG

    Parameters
    ----------
    airflow_connection_config : dict
        Airflow connection configuration, by default None
    airflow_dag_name : str
        DAG name, by default None
    airflow_dag_run_id : str
        DAG run_id, by default None
    monitoring_interval : int
        status check interval duration in seconds 
    timeout_interval_count : int, optional
        number of intervals after which the function will timeout

    Returns
    -------
    str
        DAG termination status
    """

    dag_run_status = ''
    timer = 0

    while dag_run_status not in AIRFLOW_DAG_RUN_TERMINATION_STATUSES:

        # wait for interval
        if timer > 0: sleep(monitoring_interval)

        # get DAG run status
        dag_run_status = airflow_check_dag_status(
            airflow_connection_config=airflow_connection_config,
            airflow_dag_name=airflow_dag_name,
            airflow_dag_run_id=airflow_dag_run_id
        ).get('status')

        # print status
        print(f"{datetime.now().strftime(TIMER_FORMAT)} - DAG run status: {dag_run_status}")

        # check for timeout
        if timer > timeout_interval_count:
            print(f"{datetime.now().strftime(TIMER_FORMAT)} - DAG monitor timeout")
            dag_run_status = 'timeout'
            break

        # increment timer
        timer += 1

    print(f"{datetime.now().strftime(TIMER_FORMAT)} - DAG run finished with status: {dag_run_status}")

    return dag_run_status

