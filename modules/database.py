from dept.base import *
from sqlalchemy import create_engine, text
from io import StringIO
import csv

#############################################################################
# VARIABLES
#############################################################################

DATATYPE_MAPPING = {

    "postgres": {
        "float64":"DOUBLE PRECISION",
        "int64": "BIGINT",
        "int32": "INT",
        "object": "TEXT",
        "datetime64[ns]": "TIMESTAMP",
        "bool": "BOOLEAN"
    }

}


#############################################################################
# DB METHODS
#############################################################################

def _db_connection_engine(
    db_connection_config: dict=None
    ) -> object:
    """
    creates SQLAlchemy database connection engine

    Parameters
    ----------
    db_connection_config : dict
        database connection configuration dictionary, by default None

    Returns
    -------
    object
        SQLAlchemy database connection engine
    """    

    try:
        
        # define connection string
        if db_connection_config['type'].lower() == 'postgres':
            
            connection_string = f"\
                {db_connection_config.get("user_name")}\
                :{db_connection_config.get("password")}\
                @{db_connection_config.get("host")}\
                :{db_connection_config.get("port")}\
                /{db_connection_config.get("database_name")}\
                "
            
            # create connection engine
            db_engine = create_engine(f"postgresql+psycopg2://{connection_string}")\
                .execution_options(autocommit=True)
            
            return db_engine
        
        else: 
            print(f"ERROR: connection type not recognized")
            return None
        
    except Exception as e:
        print(f"ERROR: unable to connect to database")
        print(e)

        return None
    
#############################################################################
    
@decorator_timer
def db_query(
    db_connection_config: dict=None,
    query: str=None,
    chunksize: int=None
    ) -> pd.DataFrame:
    """
    _summary_

    Parameters
    ----------
    db_connection_config : dict
        database connection configuration dictionary, by default None
    query : str
        SQL query string/file path to SQL file with a single SQL query string/database object address, by default None
    chunksize : int, optional
        record chunk size, by default None

    Returns
    -------
    pd.DataFrame
        pandas dataframe with the result set
    """

    try:

        # read sql from file
        if query[-4:].lower() == ".sql":

            with open(query) as f:
                sql_string = f.read()

        # collect everything from object
        elif re.search(f"^\w+(\.)?\w+$", query, flags= re.I):

            sql_string = f"SELECT * FROM {query}"

        else:
            sql_string = query

        # create connection engine
        db_engine = _db_connection_engine(db_connection_config)


        # collect query
        if chunksize is None: 
            df = pd.read_sql(text(sql_string), db_engine)
        else:
            df = pd.DataFrame()
            for df_chunk in pd.read_sql(text(sql_string), db_engine, chunksize=chunksize):
                df = pd.concat([df, df_chunk], ignore_index=True)

        return df
    
    except Exception as e:
        print(e)
        return None


#############################################################################

@decorator_timer
def db_execute(
    db_connection_config: dict=None,
    query: str=None,
    ) -> bool:
    """
    _summary_

    Parameters
    ----------
    db_connection_config : dict
        database connection configuration dictionary, by default None
    query : str
        SQL statement string/file path to SQL file with a single SQL statement string, by default None

    Returns
    -------
    bool
        True if statement executed successfully
    """

    try:

        # read sql from file
        if query[-4:].lower() == ".sql":

            with open(query) as f:
                sql_string = f.read()

        else:
            sql_string = query

        # create connection engine
        db_engine = _db_connection_engine(db_connection_config)

        with db_engine.connect() as c:
            c.execute(text(sql_string))

        return True
    
    except Exception as e:
        print(e)
        return False


#############################################################################

@decorator_timer
def db_create_table(
    db_connection_config: dict=None,
    data_schema: dict=None, 
    table_address: str=None, 
    ownership: str=None,
    normalize_column_names: bool=True
    ) -> bool:
    """
    creates a database table

    Parameters
    ----------
    db_connection_config : dict
        database connection configuration dictionary, by default None
    data_schema : dict
        column_name (key) + data_type (value) mapping, by default None
    table_address : str
        target table address, by default None
    ownership : str, optional
        table ownership assignment, by default None
    normalize_column_names : bool, optional
        column names normalized to lower case, special characters replaced with _

    Returns
    -------
    bool
        True if table created successfully
    """

    # join column definitions into SQL statement 
    if normalize_column_names == True:
        sql_columns_defintion = ",\n".join([f"{normalize_key(column_name)} {data_type}" for column_name, data_type in data_schema.items()])

    else:
        sql_columns_defintion = ",\n".join([f"{column_name} {data_type}" for column_name, data_type in data_schema.items()])

    # generate table definition script
    sql_table_definition = f"""
    CREATE TABLE {table_address}(
        {sql_columns_defintion}
    );
    """

    # assign ownership
    if ownership is not None:
        ownership_assignment = f"ALTER TABLE {table_address} OWNER TO {ownership};"
    else: 
        ownership_assignment = ""

    # execute statement
    sql_execute_statement = sql_table_definition + "\n" + ownership_assignment
    db_execute(db_connection_config, sql_execute_statement)

    return True


#############################################################################

def _psql_insert_copy(
    table_name: str=None,
    db_engine: object=None,
    column_names: list=None,
    data_rows   
    ):

    # gets DBAPI connection that can provide a cursor
    dbapi_connection = db_engine.connection

    with dbapi_connection.cursor() as cur:
        string_buffer = StringIO()
        writer = csv.writer(string_buffer)
        writer.writerows(data_rows)
        string_buffer.seek(0)

        data_points = ', '.join('"{}"'.format(k) for k in column_names)

        if table_name.schema:
            table_address = f'{table_name.schema}.{table_name.name}'
        else:
            table_address = table_name.name

        psql_statement = f'COPY {table_address} ({data_points}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=psql_statement, file=string_buffer)

    return True


#############################################################################

@decorator_timer
def db_upload(
    db_connection_config: dict=None,
    data: pd.DataFrame=None,
    target_table: str=None,
    if_exists: str="fail",
    chunksize: int=None,
    normalize_column_names: bool=True,
    **kwargs
    ) -> bool:
    """
    uploads data from Pandas DataFrame into a database table
    
    Parameters
    ----------
    db_connection_config : dict
        database connection configuration dictionary, by default None
    data : pd.DataFrame
        pandas dataframe, by default None
    target_table : str
        table address for upload, by default None
    if_exists : str, optional
        behaviour if target table exists, by default "fail"
        -> 'fail' - procedure fails
        -> 'replace' - existing table is dropped and new one created based on the input data
        -> 'append' 
    chunksize : int, optional
        _description_, by default None

    Returns
    -------
    bool
        True if upload executed successfully
    """

    try:
        
        # read data schema
        input_data_schema = data.dtypes.astype(str).to_dict()

        # read data mapping dictionary
        datatype_mapping = DATATYPE_MAPPING[db_connection_config['type']]

        # translate 
        sql_data_schema = {}
        for column_name, data_type in input_data_schema.items():
            sql_data_schema[column_name] = datatype_mapping[data_type] if data_type in data_type.keys() else datatype_mapping['object']
        
        # create connection engine
        db_engine = _db_connection_engine(db_connection_config)

        # check if table exists
        query = f"SELECT 1 FROM {target_table}"
        table_exists_check = db_query(db_connection_config, query)

        # check if table already exists
        if table_exists_check is None: raise Exception("unable to connect to database")
        elif table_exists_check[0,0] == 1: table_exists = True
        else: table_exists = False

        # extract table schema
        table_address = target_table.split('.')

        if len(table_address) == 2:
            table_schema, table_name = table_address
        else:
            table_schema = db_connection_config.get('default_schema')
            table_name = table_address[0]

        
        if table_exists == True:

            if if_exists == 'fail':
                raise Exception(f"table {target_table} already exists -> cancelling data upload")
            
            elif if_exists == 'replace':

                # drop table
                sql_statement = f"DROP TABLE {target_table};"
                db_execute(db_connection_config, sql_statement)

                # create table
                db_create_table(
                    db_connection_config=db_connection_config,
                    data_schema=sql_data_schema,
                    table_address=target_table,
                    db_engine = db_engine,
                    ownership=db_connection_config.get('ownership'),
                    normalize_column_names=normalize_column_names
                    )

        elif table_exists == False:

            # create table
            db_create_table(
                db_connection_config=db_connection_config,
                data_schema=sql_data_schema,
                table_address=target_table,
                db_engine = db_engine,
                ownership=db_connection_config.get('ownership'),
                normalize_column_names=normalize_column_names
                )


        # specify upload method for a given database type
        if db_connection_config['type'].lower() == "postgres":

            # upload data
            data.to_sql(
                name=table_name,
                con=db_engine,
                schema=table_schema,
                method=_psql_insert_copy,
                if_exists='append',
                index=False
                )


        return True


