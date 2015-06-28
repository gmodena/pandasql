pandasql
========

This is a fork of `pandasql` that uses SQLAlchemy (http://www.sqlalchemy.org) to handle database connections. Backward compatibility to the old dbapi backend is provided, both for in memory and on disk persistence.

`pandasql` allows you to query `pandas` DataFrames using SQL syntax. It works 
similarly to `sqldf` in R. `pandasql` seeks to provide a more familiar way of 
manipulating and cleaning data for people new to Python or `pandas`.

With SQLAlchemy `pandasql` can use a variety of backends to offload computation to. A use case for this would be working with data that does not fit in memory, or queries that produce (intermediate) result sets that do not fit in memory.

#### Installation
```
$ pip install -U git+https://github.com/gmodena/pandasql@sqlalchemy
```

#### Basics
The main function used in pandasql is `sqldf`. `sqldf` accepts 4 parametrs
   - a sql query string
   - an set of session/environment variables (`locals()` or `globals()`)
   - what to do if a table with the same name as the dataframe already exists in the database
   -  create a sqlalchemy Engine object to handle a database connection (eg. disk persistence)

Specifying `locals()` or `globals()` can get tedious. You can defined a short 
helper function to fix this.

    from pandasql import sqldf
    pysqldf = lambda q: sqldf(q, globals())

#### Querying
By default `pandasql` uses [SQLite syntax](http://www.sqlite.org/lang.html). Any `pandas` 
dataframes will be automatically detected by `pandasql`. You can query them as 
you would any regular SQL table.


```
$ python
>>> from pandasql import sqldf, load_meat, load_births
>>> pysqldf = lambda q: sqldf(q, globals())
>>> meat = load_meat()
>>> births = load_births()
>>> print pysqldf("SELECT * FROM meat LIMIT 10;").head()
                  date  beef  veal  pork  lamb_and_mutton broilers other_chicken turkey
0  1944-01-01 00:00:00   751    85  1280               89     None          None   None
1  1944-02-01 00:00:00   713    77  1169               72     None          None   None
2  1944-03-01 00:00:00   741    90  1128               75     None          None   None
3  1944-04-01 00:00:00   650    89   978               66     None          None   None
4  1944-05-01 00:00:00   681   106  1029               78     None          None   None
```

joins and aggregations are also supported
```
>>> q = """SELECT
        m.date, m.beef, b.births
     FROM
        meats m
     INNER JOIN
        births b
           ON m.date = b.date;"""
>>> joined = pyqldf(q)
>>> print joined.head()
                    date    beef  births
403  2012-07-01 00:00:00  2200.8  368450
404  2012-08-01 00:00:00  2367.5  359554
405  2012-09-01 00:00:00  2016.0  361922
406  2012-10-01 00:00:00  2343.7  347625
407  2012-11-01 00:00:00  2206.6  320195

>>> q = "select
           strftime('%Y', date) as year
           , SUM(beef) as beef_total
           FROM
              meat
           GROUP BY
              year;"
>>> print pysqldf(q).head()
   year  beef_total
0  1944        8801
1  1945        9936
2  1946        9010
3  1947       10096
4  1948        8766
```

More information and code samples available in the [examples](https://github.com/yhat/pandasql/blob/master/examples/demo.py)
 folder or on [our blog](http://blog.yhathq.com/posts/pandasql-sql-for-pandas-dataframes.html).

#### SQLAlchemy
The `engine_conf` parameter to `sqldf` is a dict whose keys are used to create a SQLAlchemy connection uri. The naming convention follows the parameter names of `sqlalchemy.engine.url.URL`.

For instance, to connect to postresql
```
>>> engine_conf = {'drivername': 'postgresql', 'username': 'postgres', 'password': 'password', 'host':'192.168.59.103', 'port':'5432', 'database': 'postgres'}
>>> pysqldf = lambda q: sqldf(q, globals(), engine_conf=engine_conf)
```

Similarly, for sqlite
```
>>> engine_conf = {'drivername': 'sqlite', 'database': '.pandasql.db'}
>>> pysqldf = lambda q: sqldf(q, globals(), engine_conf=engine_conf)
```


More information and code samples available in the [examples](https://github.com/yhat/pandasql/blob/master/examples/demo.py)
 folder or on [our blog](http://blog.yhathq.com/posts/pandasql-sql-for-pandas-dataframes.html).



[![Analytics](https://ga-beacon.appspot.com/UA-46996803-1/pandasql/README.md)](https://github.com/yhat/pandasql)
