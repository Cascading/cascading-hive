# cascading-hive

Cascading-hive is an integration between Apache Hive and Cascading. It currently
has couple major features:

- ability to run Hive queries within a Cascade
- reading from Hive tables within a Cascading Flow
- writing/creating Hive tables from a Cascading Flow
- writing/creating partitioned Hive tables from a Cascading Flow

The `demo` sub-directory contains applications, which demonstrate those
features.

The code will pick up the configuration for a remote Hive MetaStore
automatically, as long as it is present in your hive or hadoop configuration
files.

## Hive version

Currently this project targets Hive 0.10. If you are using a newer version, you
have to change the `build.gradle` file and recompile the project. This is due to
the fact that the Hive MetaStore APIs are incompatible between Hive versions and
cause crashes at runtime when versions are mixed.

## Installing

To install cascading-hive into your local maven repository do this:

    > gradle install

## Limitations

Please note that the support for Hive views is currently limited since views
pretend to be a ressource or a `Tap` in Cascading terms, but are actually
computation. It is currently not possible to read from a Hive View within a
Cascading Flow. To work around this, you can create a table in Hive instead and
read from that within your Cascading Flow. This restriction might change in the
future.

Please also note also that Hive relies on the `hadoop` command being present in
your `PATH` when it runs the queries on the Hadoop cluster.
