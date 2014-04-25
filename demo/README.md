# cascading-hive demo applications


## Building:

    # install cascading-hive into your local maven repo
    > cd ..
    > gradle install

    # go back to the demo directory and create the hadoop compliant jar
    > cd demo
    > gradle jar



## cascading.hive.HiveDemo

This is a demo application showing you how to integrate hive with cascading. The
application creates a hive table, then populates it with data from
the local file system. Then it uses the table to bootstrap a second table which
is read by a pure cascading flow and written to a third table. Finally the data
from the third table is read back via Hives JDBC support to show the seamless
integration between the two.


### Running this application:

    >  hadoop jar build/libs/cascading-hive-demo-1.0.jar cascading.hive.HiveDemo

### Local MetaStore

If you run the application against a local MetaStore it will create some files
and directories, that you should remove afterwards, if you want to run the app
again. In production deployments you will typically have a remote meta store, so
that will not happen.

    >  rm -rf metastore_db/ derby.log TempStatsStore/

## cascading.hive.HivePartitionDemo

This demo shows how to create a partitioned hive table from a Cascading flow.


### Running this application:

    >  hadoop jar build/libs/cascading-hive-demo-1.0.jar cascading.hive.HivePartitionDemo

### Remote MetaStore

This demo will only work if you are using a hosted HiveMetaStore since the
Cascading flow has to be able to register partitions in the MetaStore as they
are created.


## cascading.hive.HiveViewDemo

Demo that builds on top of the HivePartitionDemo, but creates a view via a
HiveFlow and selects data via JDBC from that view

### Running this application:

    >  hadoop jar build/libs/cascading-hive-demo-1.0.jar cascading.hive.HiveViewDemo

