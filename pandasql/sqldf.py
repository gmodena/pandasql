import sqlite3 as sqlite
import pandas as pd
import numpy as np
from pandas.io.sql import to_sql, read_sql
import re
import os,sys


def _ensure_data_frame(obj, name):
    """
    obj a python object to be converted to a DataFrame

    take an object and make sure that it's a pandas data frame
    """
    # we accept pandas Dataframe, and also dictionaries, lists, tuples
    # we'll just convert them to Pandas Dataframe
    if isinstance(obj, pd.DataFrame):
        df = obj
    elif isinstance(obj, (tuple, list)):
        # tuple and list case
        if len(obj) == 0:
            return pd.Dataframe()

        firstrow = obj[0]

        if isinstance(firstrow, (tuple, list)):
            # multiple-columns
            colnames = ["c%d" % i for i in range(len(firstrow))]
            df = pd.DataFrame(obj, columns=colnames)
        else:
            # mono-column
            df = pd.DataFrame(obj, columns=["c0"])

    if not isinstance(df, pd.DataFrame):
        raise Exception(
            "%s is not a Dataframe, tuple, list, nor dictionary" % name)

    for col in df:
        if df[col].dtype == np.int64:
            df[col] = df[col].astype(np.float)
        elif isinstance(df[col].get(0), pd.tslib.Timestamp):
            df[col] = df[col].apply(lambda x: str(x))

    return df


def _extract_table_names(q):
    "extracts table names from a sql query"
    # a good old fashioned regex. turns out this worked better
    # than actually parsing the code
    rgx = '(?:FROM|JOIN)\s+([A-Za-z0-9_]+)'
    tables = re.findall(rgx, q, re.IGNORECASE)
    return list(set(tables))


def _write_table(tablename, df, conn, flavor='sqlite', if_exists='fail'):
    "writes a dataframe to the sqlite database."

    if flavor == 'sqlite':
        for col in df.columns:
            if re.search("[() ]", col):
                msg = "please follow SQLite column naming conventions: "
                msg += "http://www.sqlite.org/lang_keywords.html"
                raise Exception(msg)

    to_sql(df, name=tablename, con=conn, flavor=flavor, if_exists=if_exists)


def _make_connection(inmemory, engine_conf):
    sqlite_detect_types = sqlite.PARSE_DECLTYPES | sqlite.PARSE_COLNAMES
    if engine_conf:
        try:
            from sqlalchemy import create_engine
            from sqlalchemy.engine.url import URL
        except ImportError:
            raise Exception(
                'An engine_conf has been specified, \
                but sqlalchemy is not installed.')
            sys.exit(1)

        engine_uri = URL(**engine_conf)

        connect_args = {}
        if engine_conf['drivername'] == 'sqlite':
            connect_args['detect_types'] = sqlite_detect_types
        flavor = engine_conf['drivername']
        dbname = engine_conf['database']
        try:
            engine = create_engine(engine_uri, connect_args=connect_args)
        except ImportError as e:
            print "ImportError: {0}".format(e)
            sys.exit(1)
    else:
        # Fallback to sqlite + dbapi
        flavor = 'sqlite'
        if inmemory:
            dbname = ":memory:"
        else:
            dbname = ".pandasql.db"
        engine = sqlite.connect(dbname, detect_types=sqlite_detect_types)

    return engine, flavor, dbname


def sqldf(q, env, inmemory=True, if_exists=None, engine_conf=None):
    """
    query pandas data frames using sql syntax

    Parameters
    ----------
    q: string
        a sql query using DataFrames as tables
    env: locals() or globals()
        variable environment; locals() or globals() in your function
        allows sqldf to access the variables in your python environment
    dbtype: bool
        memory/disk; default is in memory; if not memory then it will
        be temporarily persisted to disk. Ignored if engine_conf is non null
    if_exists: string
        what to do if a table with the same name as the dataframe already
        exists; see:
        http://pandas.pydata.org/pandas-docs/dev/generated/pandas.DataFrame.to_sql.html
        if connecting sqlite (in memory or on disk), if_exists will be set to
        'replace'; for other dialects, it will be set to 'fail'.
    engine_conf: dictionary
        parameters for instantiating sqlalchemy.engine.url.URL; it allows
        pandasql to create a sqlalchemy Engine object to handle a database
        connection

    Returns
    -------
    result: DataFrame
        returns a DataFrame with your query's result

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({
        "x": range(100),
        "y": range(100)
    })
    >>> from pandasql import sqldf
    >>> sqldf("select * from df;", globals())
    >>> sqldf("select * from df;", locals())
    >>> sqldf("select avg(x) from df;", locals())
    """

    engine, flavor, dbname = _make_connection(inmemory, engine_conf)

    # how to handle existing tables
    if not if_exists:
        if flavor == 'sqlite':
            # if_exists is set to 'replace' to keep behaviour
            # consistent with previous versions of the module
            if_exists = 'replace'
        else:
            # be conservative and don't allow tables to be overwritten
            if_exists = 'fail'

    tables = _extract_table_names(q)
    for table in tables:
        if table not in env:
            if isinstance(engine, sqlite.Connection):
                engine.close()
            else:
                engine.dispose()
            if not inmemory and flavor == 'sqlite':
                os.remove(dbname)
            raise Exception("%s not found" % table)
        df = env[table]
        df = _ensure_data_frame(df, table)
        _write_table(
            table, df, conn=engine, flavor=flavor, if_exists=if_exists)

    try:
        result = read_sql(q, engine, index_col=None)
        if 'index' in result:
            del result['index']
    except Exception:
        result = None
    finally:
        if isinstance(engine, sqlite.Connection):
            engine.close()
        else:
            engine.dispose()
        if not inmemory and flavor == 'sqlite':
            os.remove(dbname)
    return result
