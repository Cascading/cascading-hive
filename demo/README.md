# cascading-hive demo application

This is a demo application showing you how to integrate hive with cascading. The
application is first creating a table in hive, then populating it with data from
the local file system. Then it uses the table to bootstrap a second table which
is read by a pure cascading flow and written to a third table. Finally the data
from the third table is read back via Hives JDBC support to show the seamless
integration between the two.


## Building this application:

    # install cascading-hive into your local maven repo
    > cd ..
    > gradle install

    # go back to the demo directory and create the hadoop compliant jar
    > cd demo
    > gradle jar

## Running this application:

    >  hadoop jar build/libs/cascading-hive-demo-1.0.jar


## Local MetaStore

If you run the application against a local MetaStore it will create some files
and directories, that you should remove afterwards, if you want to run the app
again. In production deployments you will typically have a remote meta store, so
that will not happen.

    >  rm -rf metastore_db/ derby.log TempStatsStore/
